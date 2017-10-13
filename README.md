<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/Tempus_Logo_Black_with_TagLine.png" width="950" height="245" alt="Hashmap, Inc Tempus"/>

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

# nifi-opcua-bundle
These processors and associated controller service allow NiFi access to OPC UA servers in a read-only fashion. This bundle
provides 2 processors, GetOPCNodeList and GetOPCData. GetNodeIds allows access to the tags that are currently in the OPCUA server,
GetOPCData takes a list of tags and queries the OPC UA server for the values. The StandardOPCUAService provides the connectivity
to the OPCUA server so that multiple processors can leverage the same connection/session information.

## Table of Contents
hi
- [Features](#features)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [License](#license)

## Features

This processor aims to provide a few key features:

* Access to list the tag information currently in the OPC UA server
* Access to query the data currently in the OPC UA server
* Optional null value exclusion
* Configurable timestamp selection

## Requirements

* JDK 1.8 at a minimum
* Maven 3.1 or newer
* Git client (to build locally)
* OPC Foundation Stack (instructions to build below)

## Getting Started

### Build the OPC Foundation Stack

Clone the OPC Foundation GitHub repository

    https://github.com/OPCFoundation/UA-Java.git

Change directory into the UA-Java directory

    cd UA-Java
    
Execute the package phase (NOTE: at the time of this writing, there were test failures due to invalid tests, there are currently PR's
out there to address these, but they have not been merged into master, therefore we need to skip tests)

    mvn package -DskipTests
    
### Setup the local build environment for the processor    

To build the library and get started first off clone the GitHub repository 

    git clone https://github.com/hashmapinc/nifi-opcua-bundle.git
    
Copy the jar from the previous step where we built the OPC Foundation code from the cloned repo of the OPC foundation 
code (Where repo_location is the location of where the cloned repo is and {version} is the version of the OPC Foundation 
code that was cloned.)

    {repo_location}/UA-Java/target/opc-ua-stack-{version}-SNAPSHOT.jar

Place that file into the following directory (where repo_location is the location of where the nifi-opcua-bundle repo was cloned.)

    {repo_location}/nifi-opcua-bundle/opc-deploy-local/src/main/resources
    
Change directory into the root of the nifi-opcua-bundle codebase located in

    {repo_location}/nifi-opcua-bundle
    
Execute a maven clean install

    mvn clean install
    
A Build success message should appear
   
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 9.384 s
    [INFO] Finished at: 2017-08-01T08:27:00-05:00
    [INFO] Final Memory: 31M/423M
    [INFO] ------------------------------------------------------------------------

A NAR file should be located in the following directory

    {repo_location}/nifi-opcua-bundle/nifi-opcua-bundle/nifi-opcua-bundle-nar/target
    
Copy this NAR file to the /lib directory and restart (or start) Nifi.

## Usage

### Finding the Starting Node in the Address Space

Once NiFi is restarted the processors should be able to be added as normal, by dragging a processor onto the NiFi canvas.
You can filter the long list of processors by typing OPC in the filter box as shown below:

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/AddProcessorFilter.png" alt="AddProcessor.png"/>

Add the GetOPCNodeList processor to the canvas and configure it by right-clicking on the processor and clicking configure from
the context menu. For now, go ahead and auto-terminate the failure relationship as shown below due to this being a quick test 
(in real life, you will want to do something else with this relationship).

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/ConfigureGetIdFailureTerm.png" alt="Autoterminatefailure.png"/>

Click on the **SCHEDULING** tab and set the timer to something like 10 seconds, as below. Otherwise you will just slam your OPC
server with requests to enumerate the address space.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/TimerGetNodeListConfig.png" alt="timergetnodelist.png"/>

Next, configure the Properties by clicking on the **PROPERTIES** tab. Create a new controller service by clicking in the box to the 
right of the OPC UA Service property and clicking on Create a new service from the drop down as shown below.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/CreateNewServiceDropDown.png" alt="Createnewservice.png"/>

This will bring up the Add Controller service modal box as shown below. Leave all as default and click **Create**.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/AddControllerService.png" alt="addcontrollerservice.png"/>

Don't worry about configuring the controller service just yet, lets focus on the other properties in the GetOPCNodeList. In this step we are going
to configure the processor to allow us to visualize the list of tags within the OPC UA server. So lets go ahead and set the following properties:

| Property                | Value        | Notes                                                                                                      |
|-------------------------|--------------|------------------------------------------------------------------------------------------------------------|
| Recursive Depth         | 3            | This is how many child levels to traverse from the top                                                     |
| Starting Nodes          | No value set | This will be blank as this is what we are determining in this step                                         |
| Print Indentation       | Yes          | This just helps visualize the hierarchy in the flow file once the data is received                         |
| Remove Expanded Node ID | No           | This is for when we are generating a tag list to query, not needed in this step                            |
| Max References Per Node | 1000         | It is not necessary to set this at this stage as we are just looking for the parent node of the data tags. |

When you are done configuring the processor as per the table above, click on the little arrow to the right of the controller service (shown below). This
will allow you to configure the controller service. NiFi will ask you to save changes to the processor before continuing, click **Yes**.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/configurearrow.png" alt="addcontrollerservice.png"/>

You should be presented with a list of controller services, if you are on a fresh instance of NiFi you should only see the StandardOPCUAService controller
service that we created above. Click on the pencil to the right of the controller service that we created above. This will take you to the Configure Controller
Service modal box. Click on the **PROPERTIES** tab. and configure it as per the image below, replacing the Endpoint URL with your own opc.tcp//{ipaddress}:{port}
endpoint.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/Configure%20ControllerService.png" alt="addcontrollerservice.png"/>

Click apply to close the configuration modal. Enable it by clicking on the little lightining bolt next to the service when you are back at the controller service list.
When the Enable Controller Service Modal appears, leave the scope as service only and click **Enable**. Once it is enabled, click **Close** to close the modal. Finally,
click the **X** at the top right corner of the Modal box to return to the NiFi canvas. Go ahead and drop a GetOPCData processor on the canvas as we did with the GetOPCNodeList
processor. Don't worry about configuring it at this stage, just drop it on the canvas and create a relationship from GetOPCNodeList to GetOPCData by mousing over the GetOPCNodeList 
and when the arrow appears drag it to the GetOPCData processor and when the Create Connection modal appears
check the Success box and click **ADD**. Your flow should look like the one below.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/Flow.png" alt="addcontrollerservice.png"/>

We are now ready to get the Node list, start the GetOPCNodeList processor by right clicking and selecting **Start** from the 
context menu. Refresh the canvas by right clicking anywhere on the canvas and selecting Refresh from the context menu. You should
see 1 flowfile queued in the Success relationship. At this point you can stop the GetOPCNodeList processor as there is no reason to
keep pulling the same data. Right click on the Success relationship between the GetOPCNodeList and GetOPCData processors and choose
**List Queue** from the context menu. This will bring up a modal with the flowfiles currently in the queue. Click on the i icon
in the first column next to the flow file to bring up the content viewer window, and click the **View** button in the bottom right. 
This will show the contents of the flow file and you will look for something that has your data. (most of the http://opcfoundation.org 
stuff is system related). The data is shown in the image below.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/GetListFull.png" alt="addcontrollerservice.png"/>

This image is has a bunch of stuff above and below the snip, but this show you the root of the data as being *ns=2;s=SimulatedChannel.SimulatedDevice*.
Record this as your starting node. In the next step we will use this to reconfigure the processor to only get the tag list and then configure the GetOPCData processor.

### Getting the Data

### Reconfigure the GetOPCNodeList processor

Now that we know what our starting node should be we are ready to reconfigure our processor. Right-click on the GetOPCNodeList processor and
select **Configure** from the context menu. Click on the **PROPERTIES** tab. Enter the properties as below.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/reconfigured%20GetOPCNodeList%20processor.png" alt="addcontrollerservice.png"/>

An explanation of these different properties is in the table below:

| Property                | Value                                   | Notes                                                                                                                                                                                   |
|-------------------------|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| OPC UA Service          | StandardOPCUAService                    | Keep this the same as before                                                                                                                                                            |
| Recursive Depth         | 0                                       | Now that we know the node that contains the tag data, we don't need to traverse from the starting node anymore, so we set this to 0.                                                    |
| Print Indentation       | No                                      | Now that we know what we are looking for, we don't have to make it easy to read anymore.                                                                                                |
| Starting Nodes          | ns=2;s=SimulatedChannel.SimulatedDevice | This is the value we found in the previous step to be the root of the tree that contained the tag data.                                                                                 |
| Remove Expanded Node Id | Yes                                     | This will remove the opcfoundation header that is not a valid tag for querying.                                                                                                         |
| Max References Per Node | 1000                                    | If you have more than 1000 tags in your server you will want to increase this. NOTE, if you have a lot of tags, you might want to split the query into chunks via different processors. |

Click Apply. Now the processor is configured to simply return a tag list as shown below.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/simpletaglist.png" alt="addcontrollerservice.png"/>

You are now ready to configure the GetOPCData processor. 

### Configuring the GetOPCData processor

Head back to the NiFi canvas now, and right-click on the GetOPCData processor and select **Configure** from the context menu
to configure the processor. Go ahead and auto-terminate the failure relationship, as was done above, by checking the checkbox next to 
**Failure**. Click on the **PROPERTIES** tab and fill out the information as below.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/GetOpcDataConfig.png" alt="addcontrollerservice.png"/>

NOTE: You will want to use the same controller service instance as created above for the GetOPCNodeList processor.

The description of the properties is in the table below:

| Property           | Value                | Notes                                                                                                                                                                                     |
|--------------------|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| OPC UA Service     | StandardOPCUAService | The same instance of the controller service that was created for the GetOPCNodeList processor                                                                                             |
| Return Timestamp   | Both                 | This will return both the source and the server timestamp that was requested, the other options will just return one or the other.                                                        |
| Exclude Null Value | false                | If your server has a well known null value and you would like to prevent pulling this data, set this to true and enter that well known value into the optional Null Value String property |
| Null Value String  | <blank>              | If Exclude Null Value is set to true, then this will be the value that is used to filter out the tags.                                                                                    |

For this example we will just write the data to a file using the PutData processor. So grab that processor and add it to the canvas as before.
Configure it by putting in a valid path to a directory on your machine. Create a relationship by dragging your mouse from the GetOPCData processor to the 
PutFile processor and ticking the **Success** box.

Now your flow should look like the one below.

<img src="https://github.com/hashmapinc/hashmap.github.io/blob/master/images/tempus/opc/completeflow.png" alt="addcontrollerservice.png"/>

Now you should be able to start the flow, and have the data appear in files in the location you specified in the PutFile processor configuration.

### Next Steps

This is fine for a test, however, you would want to modify this in production use. Ideally you would have 2 flows, one that updates the tag list, and 
one that gets the data for the tags. The one that updates the tag list would run at a lower frequency. Additionally, depending on the number of tags,
the queries should be split up so that they don't overwhelm the server. 

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 

