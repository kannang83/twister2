//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.examples.arrow;

import java.io.FileOutputStream;
import java.util.Random;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class ArrowWrite {

  private Random random;
  private int entries;
  private int maxEntries;
  private long checkSum;
  private long nullEntries;
  private boolean useNullValues;

  private FileOutputStream fileOutputStream;

  private RootAllocator rootAllocator = null;
  private VectorSchemaRoot root;
  private ArrowFileWriter arrowFileWriter;

  public ArrowWrite() {
    this.maxEntries = 1024;
    this.checkSum = 0;
    random = new Random(System.nanoTime());
    this.entries = this.random.nextInt(this.maxEntries);
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
  }

  private Schema makeSchema() {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    childrenBuilder.add(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null));
    return new Schema(childrenBuilder.build(), null);
  }

  public void arrowFileWrite(String arrowFile, boolean flag) throws Exception {
    this.fileOutputStream = new FileOutputStream(arrowFile);
    Schema schema = makeSchema();
    this.root = VectorSchemaRoot.create(schema, this.rootAllocator);
    DictionaryProvider.MapDictionaryProvider provider
        = new DictionaryProvider.MapDictionaryProvider();
    if (!flag) {
      this.arrowFileWriter = new ArrowFileWriter(root, provider,
          this.fileOutputStream.getChannel());
    } else {
      this.arrowFileWriter = new ArrowFileWriter(root, provider,
          new Twister2ArrowOutputStream(this.fileOutputStream));
    }
  }
}