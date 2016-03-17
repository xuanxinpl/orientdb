/*
 *
 *  *  Copyright 2016 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.security;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityExternal;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.core.security.OPasswordValidator;
import com.orientechnologies.orient.core.security.OSecurityFactory;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.security.OSecuritySystemException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerLifecycleListener;
import com.orientechnologies.orient.server.config.OServerConfigurationManager;
import com.orientechnologies.orient.server.config.OServerEntryConfiguration;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostSecurityReload;
import com.orientechnologies.orient.server.security.OAuditingService;
import com.orientechnologies.orient.server.security.OSecurityAuthenticator;
import com.orientechnologies.orient.server.security.OSecurityComponent;


/**
 * Provides an implementation of OServerSecurity.
 * 
 * @author S. Colin Leister
 * 
 */

//public class ODefaultServerSecurity extends ORecordHookAbstract implements ODatabaseLifecycleListener, OSecurityFactory, OServerLifecycleListener, OSecuritySystemAccess, OServerSecurity
public class ODefaultServerSecurity implements OSecurityFactory, OServerLifecycleListener, OServerSecurity
{
	private boolean _Enabled = true;
	private boolean _Debug = false;

	private boolean _CreateDefaultUsers = true;
	private boolean _StorePasswords = true;


	// OServerSecurity (via OSecurityAuthenticator)
	// Some external security implementations may permit falling back to a 
	// default authentication mode if external authentication fails.
	private boolean _AllowDefault = true;

	private Object _PasswordValidatorSynch = new Object();
	private OPasswordValidator _PasswordValidator;
	
	private OSecurityComponent _ImportLDAP;
	
	private OAuditingService _AuditingService;
	
	private String _ConfigFile;
	private OServer _Server;
	private OServerConfigurationManager _ServerConfig;

	protected OServer getServer() { return _Server; }

	// The SuperUser is now only used by the ODefaultServerSecurity for self-authentication.
	private final String _SuperUser = "OSecurityModuleSuperUser";
	private String _SuperUserPassword;
	private OServerUserConfiguration _SuperUserCfg;

	// We use a list because the order indicates priority of method.
	private final List<OSecurityAuthenticator> _AuthenticatorsList = new ArrayList<OSecurityAuthenticator>();

	private ConcurrentHashMap<String, Class<?>> _SecurityClassMap = new ConcurrentHashMap<String, Class<?>>();

	
	public ODefaultServerSecurity(final OServer oServer, final OServerConfigurationManager serverCfg)
	{
		_Server = oServer;
		_ServerConfig = serverCfg;
		
      oServer.registerLifecycleListener(this);
	}

	private Class<?> getClass(final ODocument jsonConfig)
	{
		Class<?> cls = null;
		
		try
		{
			if(jsonConfig.containsField("class"))
			{	
				final String clsName = jsonConfig.field("class");
		
				if(_SecurityClassMap.containsKey(clsName))
				{
					cls = _SecurityClassMap.get(clsName);
				}
				else
				{
					cls = Class.forName(clsName);
				}
			}
		}
		catch(Throwable th)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.getClass() Throwable: ", th);
		}
		
		return cls;
	}

	// OSecuritySystem (via OServerSecurity)
	// Some external security implementations may permit falling back to a 
	// default authentication mode if external authentication fails.
	public boolean isDefaultAllowed()
	{
		if(isEnabled()) return _AllowDefault;
		else return true; // If the security system is disabled return the original system default.
	}

	// OSecuritySystem (via OServerSecurity)
	public String authenticate(final String username, final String password)
	{
		try
		{
			// It's possible for the username to be null or an empty string in the case of SPNEGO Kerberos tickets.	
			if(username != null && !username.isEmpty())
			{
				if(_Debug)
					OLogManager.instance().info(this, "ODefaultServerSecurity.authenticate() ** Authenticating username: %s", username);
			
				// This means it originates from us (used by openDatabase).
				if(username.equals(_SuperUser) && password.equals(_SuperUserPassword)) return _SuperUser;
			}
			
			synchronized(_AuthenticatorsList)
			{			
				// Walk through the list of OSecurityAuthenticators.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					if(sa.isEnabled())
					{
						String principal = sa.authenticate(username, password);
					
						if(principal != null) return principal;
					}
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.authenticate() Exception: %s", ex.getMessage());
		}
		
		return null; // Indicates authentication failed.
	}

	// OSecuritySystem (via OServerSecurity)
	// Indicates if OServer should create default users if none exist.
	public boolean areDefaultUsersCreated()
	{ 
		if(isEnabled()) return _CreateDefaultUsers;
		else return true; // If the security system is disabled return the original system default.
	}
	
	// OSecuritySystem (via OServerSecurity)
	// Used for generating the appropriate HTTP authentication mechanism.
	public String getAuthenticationHeader(final String databaseName)
	{
		String header = null;

		// Default to Basic.
		if(databaseName != null) header = "WWW-Authenticate: Basic realm=\"OrientDB db-" + databaseName + "\"";
		else header = "WWW-Authenticate: Basic realm=\"OrientDB Server\"";

		if(isEnabled())
		{
			synchronized(_AuthenticatorsList)
			{
				StringBuilder sb = new StringBuilder();
				
				// Walk through the list of OSecurityAuthenticators.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					if(sa.isEnabled())
					{
						String sah = sa.getAuthenticationHeader(databaseName);
						
						if(sah != null)
						{
							sb.append(sah);
							sb.append("\n");
						}
					}
				}
				
				if(sb.length() > 0)
				{
					header = sb.toString();
				}
			}
		}
		
		return header;
	}

	// OSecuritySystem (via OServerSecurity)
	// This will first look for a user in the security.json "users" array and then check if a resource matches.
	public boolean isAuthorized(final String username, final String resource)
	{
		if(isEnabled())
		{
			if(username == null || resource == null) return false;
			
			if(username.equals(_SuperUser)) return true;
			
			synchronized(_AuthenticatorsList)
			{		
				// Walk through the list of OSecurityAuthenticators.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					if(sa.isEnabled())
					{
						if(sa.isAuthorized(username, resource)) return true;
					}
				}
			}
		}
		
		return false;
	}

	// OSecuritySystem (via OServerSecurity)
	public boolean isEnabled() { return _Enabled; }

	// OSecuritySystem (via OServerSecurity)
	// Indicates if passwords should be stored for users.
	public boolean arePasswordsStored()
	{ 
		if(isEnabled()) return _StorePasswords;
		else return true; // If the security system is disabled return the original system default.
	}
	
	// OSecuritySystem (via OServerSecurity)
	// Indicates if the primary security mechanism supports single sign-on.
	public boolean isSingleSignOnSupported()
	{ 
		if(isEnabled())
		{
			OSecurityAuthenticator priAuth = getPrimaryAuthenticator();
		
			if(priAuth != null) return priAuth.isSingleSignOnSupported();
		}
		
		return false;
	}

	// OSecuritySystem (via OServerSecurity)
	public void validatePassword(final String password) throws OInvalidPasswordException
	{
		if(isEnabled())
		{		
			if(_PasswordValidator != null)
			{
				_PasswordValidator.validatePassword(password);
			}
		}
	}


	/***
	 * OServerSecurity Interface
	 ***/
	// OServerSecurity
	public OSecurityAuthenticator getAuthenticator(final String authMethod)
	{
		if(isEnabled())
		{
			synchronized(_AuthenticatorsList)
			{
				for(OSecurityAuthenticator am : _AuthenticatorsList)
				{
					// If authMethod is null or an empty string, then return the first OSecurityAuthenticator.
					if(authMethod == null || authMethod.isEmpty()) return am;
					
					if(am.getName() != null && am.getName().equalsIgnoreCase(authMethod)) return am;
				}
			}
		}
		
		return null;
	}

	// OServerSecurity
	// Returns the first OSecurityAuthenticator in the list.
	public OSecurityAuthenticator getPrimaryAuthenticator()
	{
		if(isEnabled())
		{
			synchronized(_AuthenticatorsList)
			{
				if(_AuthenticatorsList.size() > 0) return _AuthenticatorsList.get(0);
			}
		}
		
		return null;
	}

	// OServerSecurity
	public OServerUserConfiguration getUser(final String username)
	{
		OServerUserConfiguration userCfg = null;
		
		if(isEnabled())
		{
			if(username.equals(_SuperUser)) return _SuperUserCfg;
		
			synchronized(_AuthenticatorsList)
			{		
				// Walk through the list of OSecurityAuthenticators.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					if(sa.isEnabled())
					{
						userCfg = sa.getUser(username);
						if(userCfg != null) break;
					}
				}
			}
		}
		
		return userCfg;
	}

	// OServerSecurity
	public ODatabase<?> openDatabase(final String dbName)
	{
//		final String path = _Server.getStoragePath(dbName);
//		final ODatabaseInternal<?> db = new ODatabaseDocumentTx(path);
		
//		ODatabase<?> db = _Server.openDatabase(dbName, _SuperUser, _SuperUserPassword);
		
		ODatabase<?> db = null;
		
		if(isEnabled())
		{
			db = _Server.openDatabase(dbName, _SuperUser, "", null, true); // true indicates bypassing security.
		}
		
		return db;
	}

	// OServerSecurity
	public void registerAuditingService(final OAuditingService auditingService)
	{
		_AuditingService = auditingService;
	}

	// OServerSecurity
	public void registerSecurityClass(final String classPath, final Class<?> cls)
	{
		_SecurityClassMap.put(classPath, cls);
	}


	// OServerSecurity
	public void reload(final String cfgPath)
	{
		onBeforeDeactivate();

		_ConfigFile = cfgPath;
		
		loadBefore();
		
		// Calls loadAfter().
		onAfterActivate();
		
		if(_AuditingService != null) _AuditingService.log("Reload Security", String.format("The security configuration (%s) file has been reloaded", cfgPath));
	}



	private void createSuperUser()
	{
		if(_SuperUser == null) throw new OSecuritySystemException("ODefaultServerSecurity.createSuperUser() SuperUser cannot be null");
		
		try
		{
			// Assign a temporary password so that we know if authentication requests coming from the SuperUser are from us.
			_SuperUserPassword = OSecurityManager.instance().createSHA256(String.valueOf(new java.util.Random().nextLong()));
			
			_SuperUserCfg = new OServerUserConfiguration(_SuperUser, _SuperUserPassword, "*");
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "createSuperUser() Exception: ", ex);
		}
		
		if(_SuperUserPassword == null) throw new OSecuritySystemException("ODefaultServerSecurity Could not create SuperUser");
	}	

	private void loadAuthenticators(final ODocument authDoc)
	{
		synchronized(_AuthenticatorsList)
		{
			_AuthenticatorsList.clear(); // Clear the list on reload whether authDoc has a "methods" property or not.

			if(authDoc.containsField("authenticators"))
			{
				List<ODocument> authMethodsList = authDoc.field("authenticators");

				for(ODocument authMethodDoc : authMethodsList)
				{
					try
					{
						if(authMethodDoc.containsField("name"))
						{
							final String name = authMethodDoc.field("name");

							// defaults to enabled if "enabled" is missing
							boolean enabled = true;
							
							if(authMethodDoc.containsField("enabled"))
								enabled = authMethodDoc.field("enabled");
								
							if(enabled)
							{
								Class<?> authClass = getClass(authMethodDoc);
								
					      	if(authClass != null)
					      	{					      		
					      		if(OSecurityAuthenticator.class.isAssignableFrom(authClass) && OSecurityComponent.class.isAssignableFrom(authClass))
					      		{
					      			OSecurityAuthenticator authPlugin = (OSecurityAuthenticator)authClass.newInstance();
					      			
					      			OSecurityComponent authComp = (OSecurityComponent)authPlugin;
					      			authComp.config(_Server, _ServerConfig, authMethodDoc);
					      			_AuthenticatorsList.add(authPlugin);
					      		}
					      		else
					      		{
					      			OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuthenticators() class is not an OSecurityAuthenticator or OSecurityComponent");
					      		}
					      	}
						      else
						      {
						      	OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuthenticators() authentication class is null for %s", name);
						      }								
							}
						}
						else
						{
							OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuthenticators() authentication object is missing name");
						}
					}
					catch(Throwable ex)
					{
						OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuthenticators() Exception: ", ex);
					}
				}
			}
		}
		
	}


	/***
	 * OServerLifecycleListener Interface
	 ***/
	public void onBeforeActivate()
	{
		createSuperUser();
      
		// Default
    	_ConfigFile = OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/security.json");

		// The default "security.json" file can be overridden in the server config file.
		String securityFile = getConfigProperty("server.security.file");
		if(securityFile != null) _ConfigFile = securityFile;
		
		String ssf = OGlobalConfiguration.SERVER_SECURITY_FILE.getValueAsString();
		if(ssf != null) _ConfigFile = ssf;
		
		loadBefore();

		if(_Enabled)
		{
	      // Set this OSecurityFactory instance in OSecurityManager.
   	   OSecurityManager.instance().setSecurityFactory(this);
		}
	}
	
	// OServerLifecycleListener Interface
   public void onAfterActivate()
   {
		if(_Enabled)
		{
			loadAfter();
			
			synchronized(_AuthenticatorsList)
			{
				// Notify all the security components that the server is active.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					sa.active();
				}
			}	

			if(_AuditingService != null)
			{
				_AuditingService.active();
			}
	
			if(_ImportLDAP != null)
			{
				_ImportLDAP.active();
			}
	
			registerRESTCommands();
		}
   }

	// OServerLifecycleListener Interface
   public void onBeforeDeactivate()
   {
   	if(_Enabled)
   	{
			unregisterRESTCommands();
			
			if(_ImportLDAP != null)
			{
				_ImportLDAP.dispose();
				_ImportLDAP = null;
			}

			if(_AuditingService != null)
			{
				_AuditingService.dispose();

				// Don't set _AuditingService to null here.  If this is a reload, _AuditingService.active() will be called again.
			}

			synchronized(_AuthenticatorsList)
			{
				// Notify all the security components that the server is active.
				for(OSecurityAuthenticator sa : _AuthenticatorsList)
				{
					sa.dispose();
				}
			}		
		}
   }
   
	// OServerLifecycleListener Interface
   public void onAfterDeactivate() {}



	protected void loadBefore()
	{
		ODocument jsonConfig = loadConfig(_ConfigFile);

		if(jsonConfig != null)
		{
			if(jsonConfig.containsField("enabled"))
			{
				_Enabled = jsonConfig.field("enabled");
				
				if(_Enabled == false)
				{
					OLogManager.instance().warn(this, "ODefaultServerSecurity.loadBefore() ODefaultServerSecurity is not enabled");
				}	
			}
	
			if(jsonConfig.containsField("debug"))
			{
				_Debug = jsonConfig.field("debug");
			}
			
			if(_Enabled) loadSecurity(jsonConfig);
		}
		else
		{
			// Make sure _Enabled is set to false.
			_Enabled = false;
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadBefore() Could not load configuration: %s", _ConfigFile);
		}

	}

	protected void loadAfter()
	{
		ODocument jsonConfig = loadConfig(_ConfigFile);

		if(jsonConfig == null)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadAfter() Could not load configuration: %s", _ConfigFile);
			throw new OSecuritySystemException("ODefaultServerSecurity.loadAfter() Could not load configuration: " + _ConfigFile);
		}
		
		loadAuthMethods(jsonConfig);
		
		loadPasswordValidator(jsonConfig);
		
		loadImportLDAP(jsonConfig);
		
		loadAuditingService(jsonConfig);

		if(jsonConfig.containsField("allowDefault"))
		{
			_AllowDefault = jsonConfig.field("allowDefault");
		}
	}

	// "${ORIENTDB_HOME}/config/security.json"
	private ODocument loadConfig(final String cfgPath)
	{
		ODocument securityDoc = null;

		try
		{
			// Default
			String jsonFile = OSystemVariableResolver.resolveSystemVariables(cfgPath);
		
			File file = new File(jsonFile);
			
			if(file.exists() && file.canRead())
			{
				FileInputStream fis = null;
				
				try
				{	
					fis = new FileInputStream(file);
								
					final byte[] buffer = new byte[(int)file.length()];
					fis.read(buffer);
				
					securityDoc = (ODocument)new ODocument().fromJSON(new String(buffer), "noMap");
				}
				finally
				{
					if(fis != null) fis.close();
				}
			}
			else
			{
				OLogManager.instance().error(this, "ODefaultServerSecurity.loadConfig() Could not access the security JSON file: %s", jsonFile);
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadConfig() Exception: %s", ex.getMessage());
		}
		
		return securityDoc;
	}

	protected String getConfigProperty(final String name)
	{
		String value = null;
	
		if(_Server.getConfiguration() != null && _Server.getConfiguration().properties != null)
		{
			for(OServerEntryConfiguration p : _Server.getConfiguration().properties)
			{
				if(p.name.equals(name))
				{
					value = OSystemVariableResolver.resolveSystemVariables(p.value);
					break;
				}
			}
		}
	
		return value;
	}
	
	private void loadSecurity(final ODocument jsonConfig)
	{
		try
		{
			ODocument securityDoc = jsonConfig;
			
			if(securityDoc.containsField("createDefaultUsers"))
			{
				_CreateDefaultUsers = securityDoc.field("createDefaultUsers");
			}

			if(securityDoc.containsField("storePasswords"))
			{
				_StorePasswords = securityDoc.field("storePasswords");
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadSecurity() Exception: %s", ex.getMessage());
		}
	}

	private void loadAuthMethods(final ODocument jsonConfig)
	{
		if(jsonConfig.containsField("authentication"))
		{
			ODocument authDoc = jsonConfig.field("authentication");
			
			if(authDoc.containsField("allowDefault"))
			{
				_AllowDefault = authDoc.field("allowDefault");
			}
			
			loadAuthenticators(authDoc);
		}
	}

	private void loadPasswordValidator(final ODocument jsonConfig)
	{
		try
		{
			if(jsonConfig.containsField("passwordValidator"))
			{
				ODocument pwValidDoc = jsonConfig.field("passwordValidator");

				Class<?> cls = getClass(pwValidDoc);
				
				if(cls != null)
				{
	      		if(OPasswordValidator.class.isAssignableFrom(cls) && OSecurityComponent.class.isAssignableFrom(cls))
	      		{
	      			synchronized(_PasswordValidatorSynch)
	      			{
	      				_PasswordValidator = (OPasswordValidator)cls.newInstance();
	      				
	      				OSecurityComponent pwComp = (OSecurityComponent)_PasswordValidator;
	      				pwComp.config(_Server, _ServerConfig, pwValidDoc);
	      			}
	      		}
	      		else
	      		{
	      			OLogManager.instance().error(this, "ODefaultServerSecurity.loadPasswordValidator() class is not an OPasswordValidator or an OSecurityComponent");
	      		}
				}
				else
				{
					OLogManager.instance().error(this, "ODefaultServerSecurity.loadPasswordValidator() PasswordValidator class property is missing");
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadPasswordValidator() Exception: %s", ex.getMessage());
		}
	}

	private void loadImportLDAP(final ODocument jsonConfig)
	{
		try
		{
			if(jsonConfig.containsField("ldapImporter"))
			{
				ODocument importDoc = jsonConfig.field("ldapImporter");

				if(importDoc.containsField("enabled"))
				{
					boolean enabled = importDoc.field("enabled");
					
					if(enabled == false) return;
				}

				Class<?> cls = getClass(importDoc);
				
				if(cls != null)
				{
	      		if(OSecurityComponent.class.isAssignableFrom(cls))
	      		{
      				_ImportLDAP = (OSecurityComponent)cls.newInstance();
      				_ImportLDAP.config(_Server, _ServerConfig, importDoc);
	      		}
	      		else
	      		{
	      			OLogManager.instance().error(this, "ODefaultServerSecurity.loadImportLDAP() class is not an OImportLDAP");
	      		}
				}
				else
				{
					OLogManager.instance().error(this, "ODefaultServerSecurity.loadImportLDAP() ImportLDAP class property is missing");
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadImportLDAP() Exception: %s", ex.getMessage());
		}
	}

	private void loadAuditingService(final ODocument jsonConfig)
	{
		try
		{
			if(jsonConfig.containsField("auditing"))
			{
				ODocument auditingDoc = jsonConfig.field("auditing");

				if(auditingDoc.containsField("enabled"))
				{
					boolean enabled = auditingDoc.field("enabled");
					
					if(enabled == false) return;
				}

				if(_AuditingService != null)
				{
     				_AuditingService.config(_Server, _ServerConfig, auditingDoc);
				}
				else
				{
					OLogManager.instance().warn(this, "ODefaultServerSecurity.loadAuditingService() Auditing Service is null");
				}
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.loadAuditingService() Exception: %s", ex.getMessage());
		}
	}


	/***
	 * OSecurityFactory Interface
	 ***/
	public OSecurity newSecurity()
	{
		return new OSecurityExternal();
	}


	private void registerRESTCommands()
	{
		try
		{
			final OServerNetworkListener listener = _Server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

			if(listener != null)
			{
				// Register the REST API Command.
				listener.registerStatelessCommand(new OServerCommandPostSecurityReload(this));
			}
			else
			{
				OLogManager.instance().error(this, "ODefaultServerSecurity.registerRESTCommands() unable to retrieve Network Protocol listener.");
			}
		}
		catch(Throwable th)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.registerRESTCommands() Throwable: " + th.getMessage());
		}
	}

	private void unregisterRESTCommands()
	{
		try
		{
			final OServerNetworkListener listener = _Server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);

			if(listener != null)
			{
				listener.unregisterStatelessCommand(OServerCommandPostSecurityReload.class);
			}
			else
			{
				OLogManager.instance().error(this, "ODefaultServerSecurity.unregisterRESTCommands() unable to retrieve Network Protocol listener.");
			}
		}
		catch(Throwable th)
		{
			OLogManager.instance().error(this, "ODefaultServerSecurity.unregisterRESTCommands() Throwable: " + th.getMessage());
		}
	}
}
