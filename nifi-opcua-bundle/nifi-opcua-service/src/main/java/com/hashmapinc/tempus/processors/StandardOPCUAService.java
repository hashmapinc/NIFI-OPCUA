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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.json.JSONObject;
import org.opcfoundation.ua.application.Client;
import org.opcfoundation.ua.application.SessionChannel;
import org.opcfoundation.ua.builtintypes.*;
import org.opcfoundation.ua.common.ServiceResultException;
import org.opcfoundation.ua.core.*;
import org.opcfoundation.ua.transport.security.Cert;
import org.opcfoundation.ua.transport.security.KeyPair;
import org.opcfoundation.ua.transport.security.SecurityPolicy;
import org.opcfoundation.ua.utils.EndpointUtil;

import java.io.File;
import java.util.*;

import static org.opcfoundation.ua.utils.EndpointUtil.selectBySecurityPolicy;


@Tags({"OPC", "OPCUA", "UA"})
@CapabilityDescription("Provides session management for OPC UA processors")
public class StandardOPCUAService extends AbstractControllerService implements OPCUAService {

    // Properties
    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor
            .Builder().name("Endpoint URL")
            .description("the opc.tcp address of the opc ua server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SERVER_CERT = new PropertyDescriptor
            .Builder().name("Certificate for Server application")
            .description("Certificate in .der format for server Nifi will connect, if left blank Nifi will attempt to retreive the certificate from the server")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    public static final PropertyDescriptor SECURITY_POLICY = new PropertyDescriptor
            .Builder().name("Security Policy")
            .description("How should Nifi authenticate with the UA server")
            .required(true)
            .allowableValues("None", "Basic128Rsa15", "Basic256", "Basic256Rsa256")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor
            .Builder().name("Application Name")
            .description("The application name is used to label certificates identifying this application")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> properties;
    // Global session variables used by all processors using an instance
    private static Client opcClient = null;
    private static SessionChannel currentSession = null;
    private static EndpointDescription endpointDescription = null;
    private static ActivateSessionResponse activateSessionResponse = null;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ENDPOINT);
        props.add(SECURITY_POLICY);
        props.add(SERVER_CERT);
        props.add(APPLICATION_NAME);
        properties = Collections.unmodifiableList(props);
    }

    private double timestamp;

    private static String parseNodeTree(
            String print_indentation,
            int recursiveDepth,
            int max_recursiveDepth,
            ExpandedNodeId expandedNodeId,
            UnsignedInteger max_reference_per_node,
            ComponentLog logger) {


        StringBuilder stringBuilder = new StringBuilder();

        // Conditions for exiting this function
        // If provided node is null ( should not happen )
        if (expandedNodeId == null) {
            return null;
        }

        // Have we already reached the max depth? Exit if so
        if (recursiveDepth > max_recursiveDepth) {
            return null;
        }

        // Describe the request for given node
        BrowseDescription[] NodesToBrowse = new BrowseDescription[1];
        NodesToBrowse[0] = new BrowseDescription();
        NodesToBrowse[0].setBrowseDirection(BrowseDirection.Forward);

        // Set node to browse to given Node
        if (expandedNodeId.getIdType() == IdType.String) {

            NodesToBrowse[0].setNodeId(new NodeId(expandedNodeId.getNamespaceIndex(), (String) expandedNodeId.getValue()));
        } else if (expandedNodeId.getIdType() == IdType.Numeric) {

            NodesToBrowse[0].setNodeId(new NodeId(expandedNodeId.getNamespaceIndex(), (UnsignedInteger) expandedNodeId.getValue()));
        } else if (expandedNodeId.getIdType() == IdType.Guid) {

            NodesToBrowse[0].setNodeId(new NodeId(expandedNodeId.getNamespaceIndex(), (UUID) expandedNodeId.getValue()));
        } else if (expandedNodeId.getIdType() == IdType.Opaque) {

            NodesToBrowse[0].setNodeId(new NodeId(expandedNodeId.getNamespaceIndex(), (byte[]) expandedNodeId.getValue()));
        } else {
            // Return if no matches. Is this not a valid node?
        }

        // Form request
        BrowseRequest browseRequest = new BrowseRequest();
        browseRequest.setNodesToBrowse(NodesToBrowse);

        // Form response, make request
        BrowseResponse browseResponse = new BrowseResponse();
        try {
            browseResponse = currentSession.Browse(browseRequest.getRequestHeader(), browseRequest.getView(), max_reference_per_node, browseRequest.getNodesToBrowse());
        } catch (Exception e) {

            logger.error("failed to get browse response for " + browseRequest.getNodesToBrowse());

        }

        // Get results
        BrowseResult[] browseResults = browseResponse.getResults();

        // Retrieve reference descriptions for the result set
        // 0 index is assumed !!!
        ReferenceDescription[] referenceDesc = browseResults[0].getReferences();

        // Situation 1: There are no result descriptions because we have hit a leaf
        if (referenceDesc == null) {
            return null;
        }

        // Situation 2: There are results descriptions and each node must be parsed
        for (int k = 0; k < referenceDesc.length; k++) {

            // Print indentation
            switch (print_indentation) {

                case "Yes": {
                    for (int j = 0; j < recursiveDepth; j++) {
                        stringBuilder.append("- ");
                    }
                }
            }

            // Print the current node
            stringBuilder.append(referenceDesc[k].getNodeId() + System.lineSeparator());

            // Print the child node(s)
            String str = parseNodeTree(print_indentation, recursiveDepth + 1, max_recursiveDepth, referenceDesc[k].getNodeId(), max_reference_per_node, logger);
            if (str != null) {
                stringBuilder.append(str);
            }


        }

        return stringBuilder.toString();

        // we have exhausted the child nodes of the given node
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        final ComponentLog logger = getLogger();
        logger.info("Creating variables");
        EndpointDescription[] endpointDescriptions = null;
        KeyPair myClientApplicationInstanceCertificate = null;
        KeyPair myHttpsCertificate = null;

        // Initialize OPC UA Client

        // Load Client's certificates from file or create new certs
        logger.debug("Creating Certificates");

        if (context.getProperty(SECURITY_POLICY).getValue() == "None") {
            // Build OPC Client
            logger.info("No Security Policy requested");
            myClientApplicationInstanceCertificate = null;

        } else {

            myHttpsCertificate = Utils.getHttpsCert(context.getProperty(APPLICATION_NAME).getValue());

            // Load or create HTTP and Client's Application Instance Certificate and key
            switch (context.getProperty(SECURITY_POLICY).getValue()) {
                case "Basic128Rsa15": {
                    myClientApplicationInstanceCertificate = Utils.getCert(context.getProperty(APPLICATION_NAME).getValue(), SecurityPolicy.BASIC128RSA15);
                    break;

                }
                case "Basic256": {
                    myClientApplicationInstanceCertificate = Utils.getCert(context.getProperty(APPLICATION_NAME).getValue(), SecurityPolicy.BASIC256);
                    break;

                }
                case "Basic256Rsa256": {
                    myClientApplicationInstanceCertificate = Utils.getCert(context.getProperty(APPLICATION_NAME).getValue(), SecurityPolicy.BASIC256SHA256);
                    break;
                }
                default: {
                    myClientApplicationInstanceCertificate = null;
                    break;
                }
            }
        }

        logger.info("Creating Client");

        // Create Client
        opcClient = Client.createClientApplication(myClientApplicationInstanceCertificate);
        opcClient.getApplication().getHttpsSettings().setKeyPair(myHttpsCertificate);
        opcClient.getApplication().addLocale(Locale.ENGLISH);
        opcClient.getApplication().setApplicationName(new LocalizedText(context.getProperty(APPLICATION_NAME).getValue(), Locale.ENGLISH));
        opcClient.getApplication().setProductUri("urn:" + context.getProperty(APPLICATION_NAME).getValue());


        // if a certificate is provided
        if (context.getProperty(SERVER_CERT).getValue() != null) {
            Cert myOwnCert = null;

            // if a certificate is provided
            try {
                logger.error("Certificate Provided...getting " + context.getProperty(SERVER_CERT).getValue());
                File myCertFile = new File(context.getProperty(SERVER_CERT).getValue());
                myOwnCert = Cert.load(myCertFile);

            } catch (Exception e1) {
                logger.error("Error loading certificate " + e1.getMessage());
            }

            // Describe end point
            endpointDescription = new EndpointDescription();
            endpointDescription.setEndpointUrl(context.getProperty(ENDPOINT).getValue());
            endpointDescription.setServerCertificate(myOwnCert.getEncoded());
            endpointDescription.setSecurityMode(MessageSecurityMode.Sign);

            switch (context.getProperty(SECURITY_POLICY).getValue()) {
                case "Basic128Rsa15": {
                    endpointDescription.setSecurityPolicyUri(SecurityPolicy.BASIC128RSA15.getPolicyUri());
                    break;
                }
                case "Basic256": {
                    endpointDescription.setSecurityPolicyUri(SecurityPolicy.BASIC256.getPolicyUri());
                    break;
                }
                case "Basic256Rsa256": {
                    endpointDescription.setSecurityPolicyUri(SecurityPolicy.BASIC256SHA256.getPolicyUri());
                    break;
                }
                default: {
                    endpointDescription.setSecurityPolicyUri(SecurityPolicy.NONE.getPolicyUri());
                    logger.info("No security mode specified");
                    break;
                }
            }

        } else {
            try {
                logger.info("Discovering endpoints from" + context.getProperty(ENDPOINT).getValue());
                endpointDescriptions = opcClient.discoverEndpoints(context.getProperty(ENDPOINT).getValue());
                if (endpointDescriptions == null) {
                    logger.error("Endpoint descriptions not received.");
                    return;
                }
            } catch (ServiceResultException e1) {

                logger.error("Issue getting service endpoint descriptions: " + e1.getMessage());
            }
            switch (context.getProperty(SECURITY_POLICY).getValue()) {

                case "Basic128Rsa15": {
                    endpointDescriptions = selectBySecurityPolicy(endpointDescriptions, SecurityPolicy.BASIC128RSA15);
                    break;
                }
                case "Basic256": {
                    endpointDescriptions = selectBySecurityPolicy(endpointDescriptions, SecurityPolicy.BASIC256);
                    break;
                }
                case "Basic256Rsa256": {
                    endpointDescriptions = selectBySecurityPolicy(endpointDescriptions, SecurityPolicy.BASIC256SHA256);
                    break;
                }
                default: {
                    endpointDescriptions = selectBySecurityPolicy(endpointDescriptions, SecurityPolicy.NONE);
                    logger.info("No security mode specified");
                    break;
                }
            }

            // set the provided end point url to match the given one ( for local host problem )
            endpointDescription = EndpointUtil.selectByUrl(endpointDescriptions, context.getProperty(ENDPOINT).getValue())[0];
        }

        logger.debug("Initialization Complete");

        // Create and activate session

        logger.debug("Using endpoint: " + endpointDescription.toString());

        try {


            currentSession = opcClient.createSessionChannel(endpointDescription);
            activateSessionResponse = currentSession.activate();

            timestamp = System.currentTimeMillis();

        } catch (ServiceResultException e) {
            logger.debug("Error while creating initial SessionChannel: ");
            logger.error(e.getMessage());
        }


        logger.debug("OPC UA client session ready");

    }

    public boolean updateSession() {

        final ComponentLog logger = getLogger();
        double elapsedTime = System.currentTimeMillis() - timestamp;
        if (elapsedTime < 0) {
            logger.debug("not a valid timestamp");
            return false;
        }
        if ((elapsedTime) < currentSession.getSession().getSessionTimeout()) {

            timestamp = System.currentTimeMillis();

            return true;

        } else {
            try {

                // TODO future should support multi session management
                currentSession = opcClient.createSessionChannel(endpointDescription);
                currentSession.activate();

                timestamp = System.currentTimeMillis();

                return true;

            } catch (ServiceResultException e) {
                logger.error("Error while creating new session: " + e.getMessage());
                return false;
            }
        }
    }

    @OnDisabled
    public void shutdown() {
        // Close the session
        final ComponentLog logger = getLogger();

        try {
            if (currentSession != null)
                currentSession.close();
        } catch (ServiceResultException e) {
            logger.error("Error shutting down the session - " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error shutting down the session - " + e.getMessage());
        }

    }

    @Override
    public byte[] getValue(List<String> reqTagnames, String returnTimestamp, String excludeNullValue, String nullValueString, String dataFormat, boolean longTimestamp) throws ProcessException {
        final ComponentLog logger = getLogger();

        //Create the nodes to read array
        ReadValueId nodesToRead[] = new ReadValueId[reqTagnames.size()];

        for (int i = 0; i < reqTagnames.size(); i++) {
            try {
                nodesToRead[i] = (new ReadValueId(NodeId.parseNodeId(reqTagnames.get(i)), Attributes.Value, null, null));
            } catch (Exception ex) {
                logger.error("error reading nodeId for" + reqTagnames.get(i));
            }
        }

        String serverResponse = "";

        // Form OPC request
        ReadRequest req = new ReadRequest();
        req.setMaxAge(500.00);
        req.setTimestampsToReturn(TimestampsToReturn.Both);
        req.setRequestHeader(null);
        req.setNodesToRead(nodesToRead);

        // Submit OPC Read and handle response
        try {
            ReadResponse readResponse = currentSession.Read(req);
            DataValue[] values = readResponse.getResults();

            // Validate response
            if (values != null) {
                if (values.length == 0) {
                    logger.error("OPC Server returned nothing.");
                } else {
                    // Build Response according to Data Format
                    switch (dataFormat) {
                        case "CSV" :
                            serverResponse = getDataInCSV(nodesToRead, values, returnTimestamp, excludeNullValue, nullValueString, longTimestamp);
                            serverResponse.trim();
                            break;
                        case "JSON" :
                            serverResponse = getDataInJSON(nodesToRead, values, returnTimestamp, excludeNullValue, nullValueString, longTimestamp);
                            break;
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Error parsing OPC Server Results: " + e.getMessage() + Arrays.toString(e.getStackTrace()));
        }

        return serverResponse.getBytes();
    }

    @Override
    public String getNameSpace(String print_indentation, int max_recursiveDepth, List<ExpandedNodeId> expandedNodeIds, UnsignedInteger max_reference_per_node) throws ProcessException {

        final ComponentLog logger = getLogger();
        StringBuilder stringBuilder = new StringBuilder();

        for (ExpandedNodeId expNodeId : expandedNodeIds) {
            // Set the starting node and parse the node tree
            logger.debug("Parse the result list for node " + expNodeId.toString());
            stringBuilder.append(parseNodeTree(print_indentation, 0, max_recursiveDepth, expNodeId, max_reference_per_node, logger));
        }

        return stringBuilder.toString();

    }

    private String getDataInCSV(ReadValueId nodesToRead[], DataValue values[], String returnTimestamp, String excludeNullValue, String nullValueString, boolean longTimestamp) {
        String serverResponse = "";
        for (int i = 0; i < values.length; i++) {
            String valueLine = "";
            try {
                // Build flowfile line
                if (excludeNullValue.equals("true") && values[i].getValue().toString().equals(nullValueString)) {
                    getLogger().debug("Null value returned for " + values[i].getValue().toString() + " -- Skipping because property is set");
                    continue;
                }

                valueLine += nodesToRead[i].getNodeId().toString() + ",";
                valueLine += getTimeStamp(values[i], returnTimestamp, longTimestamp) + ",";
                valueLine += values[i].getValue().toString() + ","
                          + values[i].getStatusCode().getValue().toString()
                          + System.getProperty("line.separator");

            } catch (Exception ex) {
                getLogger().error("Error parsing result for" + nodesToRead[i].getNodeId().toString());
                valueLine = "";
            }
            if (valueLine.isEmpty())
                continue;

            serverResponse += valueLine;
        }
        return serverResponse;
    }

    private String getDataInJSON(ReadValueId nodesToRead[], DataValue values[], String returnTimestamp, String excludeNullValue, String nullValueString, boolean longTimestamp) {
        JSONObject jsonObject = new JSONObject();
        Object ts = null;
        for (int i = 0; i < values.length; i++) {
            try {
                // Add JSON Object for sensor values
                if (excludeNullValue.equals("true") && values[i].getValue().toString().equals(nullValueString)) {
                    getLogger().debug("Null value returned for " + values[i].getValue().toString() + " -- Skipping because property is set");
                    continue;
                }

                ts = getTimeStamp(values[i], returnTimestamp, longTimestamp);
                String[] key = nodesToRead[i].getNodeId().toString().split("\\.");
                jsonObject.put(key[key.length - 1], values[i].getValue().toString());

            } catch (Exception ex) {
                getLogger().error("Error parsing result for" + nodesToRead[i].getNodeId().toString());
            }
        }

        // Building JSON Data
        JSONObject finalJsonObject = new JSONObject()
                                    .put("ts", ts)
                                    .put("values", jsonObject);

        return finalJsonObject.toString();
    }

    private Object getTimeStamp(DataValue value, String returnTimestamp, boolean longTimestamp) throws Exception{
        Object ts = null;
        // Get Timestamp
        try {
            if (returnTimestamp.equals("ServerTimestamp")) {
                if (longTimestamp) {
                    ts = value.getServerTimestamp().getTimeInMillis();
                } else {
                    ts = value.getServerTimestamp().toString();
                }
            }
            if (returnTimestamp.equals("SourceTimestamp")) {
                if (longTimestamp) {
                    ts = value.getSourceTimestamp().getTimeInMillis();
                } else {
                    ts = value.getSourceTimestamp().toString();
                }
            }
        } catch (Exception ex) {
            throw ex;
        }
        return ts;
    }

    private boolean validateEndpoint(Client client, String security_policy, String discoveryServer, String url) {

        // TODO This method should provide feedback
        final ComponentLog logger = getLogger();

        // Retrieve end point list
        EndpointDescription[] endpoints = null;

        // This assumes the provided url is co-served with the discovery server
        try {
            endpoints = client.discoverEndpoints(discoveryServer);
        } catch (ServiceResultException e1) {
            logger.error(e1.getMessage());
        }

        // Finally confirm the provided endpoint is in the list of
        endpoints = EndpointUtil.selectByUrl(endpoints, url);

        logger.debug(endpoints.length + "endpoints found");

        // There should only be one item left in the list
        // TODO Servers with multiple nic cards have more than one left in the list
        if (endpoints.length == 0) {
            logger.debug("No suitable endpoint found from " + url);
            return false;
        }
        return true;

    }


}
