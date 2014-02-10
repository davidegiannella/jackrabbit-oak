package org.apache.jackrabbit.oak.plugins.index.property;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class OrderedPropertyIndexEditorProvider implements IndexEditorProvider {
   public final static String TYPE = "ordered";
   
   @Override
   @CheckForNull
   public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition, @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback) throws CommitFailedException {
      Editor _editor = (TYPE.equals(type)) ? new OrderedPropertyIndexEditor(definition,root,callback) : null;
      return _editor; 
   }
}
