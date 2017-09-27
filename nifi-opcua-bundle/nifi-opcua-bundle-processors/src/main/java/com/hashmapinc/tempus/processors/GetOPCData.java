/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hashmapinc.tempus.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Tags({"OPC", "OPCUA", "UA"})
@CapabilityDescription("Fetches a response from an OPC UA server based on configured name space and input item names")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(Requirement.INPUT_REQUIRED)

public class GetOPCData extends AbstractProcessor {

    private final AtomicReference<String> timestamp = new AtomicReference<>();
    private final AtomicReference<String> excludeNullValue = new AtomicReference<>();
    private final AtomicReference<String> nullValueString = new AtomicReference<>();

	public static final PropertyDescriptor OPCUA_SERVICE = new PropertyDescriptor.Builder()
			  .name("OPC UA Service")
			  .description("Specifies the OPC UA Service that can be used to access data")
			  .required(true)
			  .identifiesControllerService(OPCUAService.class)
			  .build();
    
	public static final PropertyDescriptor RETURN_TIMESTAMP = new PropertyDescriptor
            .Builder().name("Return Timestamp")
            .description("Allows to select the source, server, or both timestamps")
            .required(true)
            .allowableValues("SourceTimestamp", "ServerTimestamp")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	 
	public static final PropertyDescriptor EXCLUDE_NULL_VALUE = new PropertyDescriptor
	            .Builder().name("Exclude Null Value")
	            .description("Return data only for non null values")
	            .required(true)
	            .allowableValues("true", "false")
	            .defaultValue("false")
	            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
	            .build();

    public static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor
            .Builder().name("DATA_FORMAT")
            .displayName("Data Format")
            .description("The format the data should be in, either CSV or JSON")
            .required(true)
            .allowableValues("JSON","CSV")
            .defaultValue("JSON")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LONG_TIMESTAMP = new PropertyDescriptor
            .Builder().name("LONG_TIMESTAMP")
            .displayName("Use Long Timestamp")
            .description("If True it will use number of milliseconds from the epoch, if not it will use an ISO8601 compatable timestamp.")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor NULL_VALUE_STRING = new PropertyDescriptor
            .Builder().name("Null Value String")
            .description("If removing null values, what string is used for null")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	 
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful OPC read")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed OPC read")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(OPCUA_SERVICE);
        descriptors.add(RETURN_TIMESTAMP);
        descriptors.add(EXCLUDE_NULL_VALUE);
        descriptors.add(NULL_VALUE_STRING);
        descriptors.add(DATA_FORMAT);
        descriptors.add(LONG_TIMESTAMP);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        timestamp.set(context.getProperty(RETURN_TIMESTAMP).getValue());
        excludeNullValue.set(context.getProperty(EXCLUDE_NULL_VALUE).getValue());
        nullValueString.set(context.getProperty(NULL_VALUE_STRING).getValue());
	}

    /* (non-Javadoc)
     * @see org.apache.nifi.processor.AbstractProcessor#onTrigger(org.apache.nifi.processor.ProcessContext, org.apache.nifi.processor.ProcessSession)
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	
    	final ComponentLog logger = getLogger();
    	
    	// Initialize  response variable
        final AtomicReference<List<String>> requestedTagnames = new AtomicReference<>();

        // get FlowFile
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        // Read tag name from flow file content
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
            	
                try{
                	List<String> tagname = new BufferedReader(new InputStreamReader(in))
                	.lines().collect(Collectors.toList());
                	
                    requestedTagnames.set(tagname);
                    
                }catch (Exception e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
        			logger.error("Failed to read");
        		}
        		
            }
            
        });
        
        // Submit to getValue
        OPCUAService opcUAService;

        try {
            opcUAService = context.getProperty(OPCUA_SERVICE)
                    .asControllerService(OPCUAService.class);
        }
        catch (Exception ex){
            logger.error(ex.getMessage());
            return;
        }


        if(opcUAService.updateSession()){
        	logger.debug("Session current");
        }else {
        	logger.debug("Session update failed");
        }

        byte[] values = opcUAService.getValue(requestedTagnames.get(),timestamp.get(),excludeNullValue.get(),nullValueString.get(), context.getProperty(DATA_FORMAT).getValue(), context.getProperty(LONG_TIMESTAMP).asBoolean());

        // Updating content-type if Data format is JSON
        if (context.getProperty(DATA_FORMAT).getValue().toString().equals("JSON")) {
            session.putAttribute(flowFile, "mime.type", "application/json");
        }
        
  		// Write the results back out to flow file
        try{
        flowFile = session.write(flowFile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(values);
            }
            
        });
        
        session.transfer(flowFile, SUCCESS);
        } catch (ProcessException ex) {
        	logger.error("Unable to process", ex);
        	session.transfer(flowFile, FAILURE);
        }
    }
    
}
