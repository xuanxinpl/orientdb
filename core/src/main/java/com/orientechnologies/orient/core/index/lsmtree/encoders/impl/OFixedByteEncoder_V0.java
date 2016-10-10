/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.index.lsmtree.encoders.impl;

import com.orientechnologies.orient.core.index.lsmtree.encoders.OByteEncoder;

/**
 * @author Sergey Sitnikov
 */
public class OFixedByteEncoder_V0 implements OByteEncoder {

  @Override
  public int version() {
    return 0;
  }

  @Override
  public int minimumSize() {
    return 1;
  }

  @Override
  public int maximumSize() {
    return 1;
  }

  @Override
  public int exactSize(Byte value) {
    return 1;
  }

  @Override
  public void encode(Byte value, Stream stream) {
    encodeByte(value, stream);
  }

  @Override
  public int exactSizeInStream(Stream stream) {
    return 1;
  }

  @Override
  public Byte decode(Stream stream) {
    return decodeByte(stream);
  }

  @Override
  public int exactSizeByte(byte value) {
    return 1;
  }

  @Override
  public void encodeByte(byte value, Stream stream) {
    stream.write(value);
  }

  @Override
  public byte decodeByte(Stream stream) {
    return stream.read();
  }

}
