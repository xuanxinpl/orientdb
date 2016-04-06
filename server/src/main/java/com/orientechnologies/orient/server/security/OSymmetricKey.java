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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.security.AlgorithmParameters;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;
import java.util.UUID;

import javax.crypto.Cipher;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

//import sun.misc.BASE64Decoder;
//import sun.misc.BASE64Encoder;
 
public class OSymmetricKey
{
	// These are just defaults.
	private String passwordKeyAlgorithm = "PBKDF2WithHmacSHA1";
	
	// This may be overridden in the configuration.
	private String seedPhrase = UUID.randomUUID().toString();

	// Holds the length of the salt byte array.
	private int saltLength = 64;
		
	// Holds the default number of iterations used.  This may be overridden in the configuration.
	private int iteration = 65536;
	
	private String secretKeyAlgorithm = "AES";

	private String cipherTransformation = "AES/CBC/PKCS5Padding";
	
	// Holds the size of the key (in bits).
	private int keySize = 128;
	
	private SecretKey secretKey;
	private byte[] initVector;
	
	// Holds the decryption Cipher.
	private Cipher decCipher;
	// Holds the encryption Cipher.
	private Cipher encCipher;
	
	public OSymmetricKey()
	{
	}
   
	public OSymmetricKey(final String secretKeyAlgorithm, final String cipherTransform, final int keySize) throws OKeyException
	{
		this.secretKeyAlgorithm = secretKeyAlgorithm;
		this.cipherTransformation = cipherTransform;
		this.keySize = keySize;

		create();
	}

	// Setters
	public OSymmetricKey setSeedAlgorithm(final String algorithm) { passwordKeyAlgorithm = algorithm; return this; }
	public OSymmetricKey setSeedPhrase(final String phrase) { seedPhrase = phrase; return this; }
	public OSymmetricKey setSaltLength(int length) { saltLength = length; return this; }
	public OSymmetricKey setIteration(int iteration) { this.iteration = iteration; return this; }
	public OSymmetricKey setKeyAlgorithm(final String algorithm) { secretKeyAlgorithm = algorithm; return this; }
	public OSymmetricKey setCipherTransform(final String transform) { cipherTransformation = transform; return this; }	
	public OSymmetricKey setKeySize(int bits) { keySize = bits; return this; }    

	public void create() throws OKeyException
	{
		try
		{
			SecureRandom secureRandom = new SecureRandom();
			byte[] salt = secureRandom.generateSeed(saltLength);
			
			KeySpec keySpec = new PBEKeySpec(seedPhrase.toCharArray(), salt, iteration, keySize);

			SecretKeyFactory factory = SecretKeyFactory.getInstance(passwordKeyAlgorithm);
			SecretKey tempKey = factory.generateSecret(keySpec);

			secretKey = new SecretKeySpec(tempKey.getEncoded(), secretKeyAlgorithm);

			encCipher = Cipher.getInstance(cipherTransformation);
			encCipher.init(Cipher.ENCRYPT_MODE, secretKey);
			
			initVector = encCipher.getParameters().getParameterSpec(IvParameterSpec.class).getIV();
						
			decCipher = Cipher.getInstance(cipherTransformation);
			decCipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(initVector));			
		}
		catch(Exception ex)
		{
			throw OException.wrapException(new OKeyException("OSymmetricKey.create() Exception"), ex);
		}		
	}

	private static String convertToBase64(final byte[] bytes)
	{
//		return new BASE64Encoder().encode(bytes);

		String result = null;

		try
		{
			result = OBase64Utils.encodeBytes(bytes);
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(null, "convertToBase64() Exception: %s", ex.getMessage());
		}	
		
		return result;
	}
 
	private static byte[] convertFromBase64(final String base64)
	{
		byte[] result = null;
		
		try
		{
//			result = new BASE64Decoder().decodeBuffer(base64);

			if(base64 != null)
			{
				result = OBase64Utils.decode(base64.getBytes("UTF8"));
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(null, "convertFromBase64() Exception: %s", ex.getMessage());
		}	

		return result;
	}

	/**
	 * This method encrypts the provided String using the current cipher and returns it as a Base64-encoded String.
	 *
	 * @param value The String to be encrypted.
	 *
	 * @return A Base64-encoded String of the encrypted value or null if unsuccessful.
	 *
	 */
	public String encryptToBase64(String value)
	{
		String result = null;
		
		try
		{
			byte[] encrypted = encrypt(value.getBytes("UTF8"));
			result = convertToBase64(encrypted);
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "encryptToBase64() UnsupportedEncodingException: %s", ex.getMessage());
		}
		
		return result;
	}
 
	/**
	 * This method encrypts an array of bytes.
	 *
	 * @param bytes The array of bytes to be encrypted.
	 *
	 * @return The encrypted bytes or null if unsuccessful.
	 *
	 */
	public byte[] encrypt(byte[] bytes)
	{
		byte[] result = null;
		
		try
		{		
			result = encCipher.doFinal(bytes);
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "encrypt() Exception: %s", ex.getMessage());
		}
		
		return result;
	}
 
	/**
	 * This method decrypts the provided Base-64 String using the current cipher and returns it as a regular String.
	 *
	 * @param base64 The Base-64 String to be decrypted.
	 *
	 * @return The decrypted String or null if unsuccessful.
	 *
	 */
	public String decryptFromBase64(String base64)
	{
		String result = null;
		
		try
		{
			byte[] decrypted = decrypt(convertFromBase64(base64));
			
			if(decrypted != null)
			{
				result = new String(decrypted, "UTF8");
			}
			else
			{
				OLogManager.instance().error(this, "decryptFromBase64() decrypted is null"); 
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "decryptFromBase64() Exception: %s", ex.getMessage());
		}
		
		return result;
	}
	
	/**
	 * This method decrypts an array of bytes using the current cipher.
	 *
	 * @param bytes The array of bytes to be decrypted.
	 *
	 * @return The decrypted array of bytes or null if unsuccessful.
	 *
	 */
	public byte[] decrypt(byte[] bytes)
	{
		byte[] result = null;
		
		try
		{
			if(bytes != null)
			{
				result = decCipher.doFinal(bytes);
			}
			else
			{
				OLogManager.instance().debug(this, "decrypt() bytes is null");
			}
		}
		catch(Exception ex)
		{
			OLogManager.instance().error(this, "decrypt() Exception: %s", ex.getMessage());
		}
		
		return result;
	}

	public void loadKey(final ODocument jsonDoc) throws OKeyException
	{
		// clear values?
		
		
		if(jsonDoc == null) throw new OKeyException("OSymmetricKey.loadKey() JSON document is null");
		
		if(!jsonDoc.containsField("keyAlgorithm")) throw new OKeyException("OSymmetricKey.loadKey() keyAlgorithm is required");
		if(!jsonDoc.containsField("transformation")) throw new OKeyException("OSymmetricKey.loadKey() transformation is required");
		if(!jsonDoc.containsField("key")) throw new OKeyException("OSymmetricKey.loadKey() key is required");
		if(!jsonDoc.containsField("initVector")) throw new OKeyException("OSymmetricKey.loadKey() initVector is required");

		try
		{
			final String keyAlgorithm	= jsonDoc.field("keyAlgorithm");
			final String transform		= jsonDoc.field("transformation");
			final String key				= jsonDoc.field("key");
			final String ivString		= jsonDoc.field("initVector");
	
			final byte[] keyBytes = convertFromBase64(key);
			final byte[] initVector = convertFromBase64(ivString);

			SecretKey secretKey = new SecretKeySpec(keyBytes, keyAlgorithm);
			
			Cipher encCipher = Cipher.getInstance(transform);
			encCipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(initVector));
			
			Cipher decCipher = Cipher.getInstance(transform);
			decCipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(initVector));

			// Ciphers were successfully created, update the member variables.
			this.secretKeyAlgorithm = keyAlgorithm;
			this.cipherTransformation = transform;
			this.initVector = initVector;
			this.secretKey = secretKey;
			this.encCipher = encCipher;
			this.decCipher = decCipher;
		}
		catch(Exception ex)
		{
			throw OException.wrapException(new OKeyException("OSymmetricKey.loadKey() Exception"), ex);
		}
	}

	/**
	 * This method loads a key from a JSON String.
	 *
	 * @param json The String containing the key in JSON.
	 *
	 */
	public void loadFromString(final String json) throws OKeyException
	{
		ODocument jsonDoc = null;
		
		try
		{
			jsonDoc = new ODocument().fromJSON(json, "noMap");
		}
		catch(Exception ex)
		{
			throw OException.wrapException(new OKeyException("OSymmetricKey.loadFromString() Exception"), ex);
		}
		
		loadKey(jsonDoc);
	}
	
	public void loadFromFile(final String path) throws OKeyException
	{
		File file = new File(path);
		
		if(!file.exists()) throw new OKeyException("OSymmetricKey.loadFromFile() File does not exist: " + path);
		if(!file.canRead()) throw new OKeyException("OSymmetricKey.loadFromFile() File cannot be read: " + path);
		
		FileInputStream fis = null;
		
		try
		{
			fis = new FileInputStream(file);

			final byte[] buffer = new byte[(int)file.length()];
			fis.read(buffer);
			
			loadFromString(new String(buffer));
		}
		catch(FileNotFoundException fnfe)
		{
			throw OException.wrapException(new OKeyException("OSymmetricKey.loadFromFile() Exception"), fnfe);
		}
		catch(IOException ioe)
		{
			throw OException.wrapException(new OKeyException("OSymmetricKey.loadFromFile() Exception"), ioe);
		}
		finally
		{
		 	if(fis != null) try { fis.close(); } catch(IOException ioe) { }		 		 
		}
	}
	
	public String saveAsString() throws OKeyException
	{
		String result = null;
		
		if(this.secretKey == null) throw new OKeyException("OSymmetricKey.saveAsString() SecretKey is null");
		if(this.initVector == null) throw new OKeyException("OSymmetricKey.saveAsString() Initialization Vector is null");
		
		try
		{		
			final ODocument json = new ODocument();			
			json.field("keyAlgorithm", this.secretKeyAlgorithm);
			json.field("transformation", this.cipherTransformation);
			
			json.field("key", convertToBase64(this.secretKey.getEncoded()));
			json.field("initVector", convertToBase64(this.initVector));
			
//			json.save();
			
			result = json.toJSON("prettyPrint=true");
		}
		catch(Exception ex)
		{
			throw OException.wrapException(new OKeyException("OSymmetricKey.saveAsString() Exception"), ex);
		}
		
		return result;
	}
	
	public void saveToFile(final String path) throws OKeyException
	{
		File file = new File(path);
		
		if(file.exists() && !file.canWrite()) throw new OKeyException("OSymmetricKey.saveToFile() Cannot write to file: " + path);
		
		try
		{
			if(!file.exists())
			{
				file.createNewFile();
			}

			FileOutputStream fos = new FileOutputStream(file);
			
			String jsonKey = saveAsString();
			
			if(jsonKey != null)
			{
				OutputStreamWriter osw = new OutputStreamWriter(fos);
				osw.write(jsonKey);
				osw.flush();
			}

			fos.close();
		}
		catch(Exception ex)
		{
			throw OException.wrapException(new OKeyException("OSymmetricKey.saveToFile() Exception"), ex);
		}
	}
/*
	public static void main(String[] args) throws Exception
	{
		String message = "MESSAGE";
		String password = "PASSWORD";
*/
/*		
		OSymmetricKey keyTool = new OSymmetricKey();
		
		String encrypted = keyTool.encrypt(message);
		String decrypted = keyTool.decrypt(encrypted);
		
		System.out.println("Encrypt(\"" + message + "\", \"" + password + "\") = \"" + encrypted + "\"");
		System.out.println("Decrypt(\"" + encrypted + "\", \"" + password + "\") = \"" + decrypted + "\"");
		
		System.out.println("writeKey\n");
		
		keyTool.writeKey("test.key");
		
		System.out.println("readKey\n");
	*/	
/*
		OSymmetricKey keyTool = new OSymmetricKey();	
		keyTool.loadFromFile("test.key");
		
		System.out.println("3\n");
		
		String encrypted = keyTool.encryptToBase64(message);
		
		String decrypted = keyTool.decryptFromBase64(encrypted);
		
		System.out.println("5\n");
		
		
		System.out.println("Encrypt(\"" + message + "\", \"" + password + "\") = \"" + encrypted + "\"");
		System.out.println("Decrypt(\"" + encrypted + "\", \"" + password + "\") = \"" + decrypted + "\"");

    }*/
}