package org.apache.jackrabbit.oak.plugins.index.property;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

public class OrderedPropertyIndexEditorTest {
   
   @Test public void isProperlyConfiguredWithPropertyNames(){
      NodeBuilder definition = createNiceMock(NodeBuilder.class);
      PropertyState names = createNiceMock(PropertyState.class);
      expect(names.count()).andReturn(1);
      expect(definition.getProperty(IndexConstants.PROPERTY_NAMES)).andReturn(names).anyTimes();
      replay(names);
      replay(definition);
      
      OrderedPropertyIndexEditor ie = new OrderedPropertyIndexEditor(definition, null, null);
      assertFalse("With empty or missing property the index should not work.",ie.isProperlyConfigured());
   }
   
   @Test public void isProperlyConfiguredSingleValuePropertyNames(){
      NodeBuilder definition = createNiceMock(NodeBuilder.class);
      PropertyState names = createNiceMock(PropertyState.class);
      expect(names.count()).andReturn(1);
      expect(names.getValue(Type.NAME,0)).andReturn("jcr:lastModified").anyTimes();
      expect(definition.getProperty(IndexConstants.PROPERTY_NAMES)).andReturn(names).anyTimes();
      replay(names);
      replay(definition);
      
      OrderedPropertyIndexEditor ie = new OrderedPropertyIndexEditor(definition, null, null);
      assertNotNull("With a correct proprety set 'propertyNames' can't be null",ie.getPropertyNames());
      assertEquals(1,ie.getPropertyNames().size());
      assertEquals("jcr:lastModified",ie.getPropertyNames().iterator().next());
      assertTrue("Expecting a properly configured index",ie.isProperlyConfigured());
   }
   
   @Test public void multiValueProperty(){
      NodeBuilder definition = createNiceMock(NodeBuilder.class);
      PropertyState names = createNiceMock(PropertyState.class);
      expect(names.isArray()).andReturn(true).anyTimes();
      expect(names.count()).andReturn(2).anyTimes();
      expect(names.getValue(Type.NAME,0)).andReturn("jcr:lastModified").anyTimes();
      expect(names.getValue(Type.NAME,1)).andReturn("foo:bar").anyTimes();
      expect(names.getValue(Type.NAMES)).andReturn(Arrays.asList(new String[]{"jcr:lastModified","foo:bar"})).anyTimes();
      expect(definition.getProperty(IndexConstants.PROPERTY_NAMES)).andReturn(names).anyTimes();
      replay(names);
      replay(definition);

      OrderedPropertyIndexEditor ie = new OrderedPropertyIndexEditor(definition, null, null);
      assertNotNull("With a correct proprety set 'propertyNames' can't be null",ie.getPropertyNames());
      assertEquals("When multiple properties are a passed only the first one is taken", 1,ie.getPropertyNames().size());
      assertEquals("jcr:lastModified",ie.getPropertyNames().iterator().next());
      assertTrue("Expecting a properly configured index",ie.isProperlyConfigured());
   }   
}
