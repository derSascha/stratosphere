# Stratosphere Streaming Distribution

_"Big Data looks tiny from Stratosphere."_

Stratosphere-Streaming is a modification of the official Stratosphere distribution, optimized for stream processing applications. The goal is to provide and enforce QoS guarantees (latency, throughput) on large scale streaming workflows with hundreds of compute nodes.

Currently, it is implemented as a plugin and patchset inside Stratosphere's Nephele layer. Stratosphere's PACT layer does not suit itself to stream processing and has been removed.

## Getting Started
Below are three short tutorials that guide you through the first steps: Building, running and developing.

###  Build From Source

This tutorial shows how to build Stratosphere on your own system. Please open a bug report if you have any troubles!

#### Requirements
* Unix-like environment (We use Linux, Mac OS X, Cygwin)
* git
* Maven (at least version 3.0.4)
* Java 6 or 7

```
git clone https://github.com/bjoernlohrmann/stratosphere.git
cd stratosphere
mvn -DskipTests clean package # this will take up to 5 minutes
```

Stratosphere-Streaming is now installed in `stratosphere-dist/target`
If you’re a Debian/Ubuntu user, you’ll find a .deb package. We will continue with the generic case.

	cd stratosphere-dist/target/stratosphere-dist-streaming-git-bin/stratosphere-streaming-git/

The directory structure here looks like the contents of the official release distribution.

#### Build for different Hadoop Versions
This section is for advanced users that want to build Stratosphere for a different Hadoop version, for example for Hadoop Yarn support.

We use the profile activation via properties (-D).

##### Build hadoop v1 (default)
Build the default (currently hadoop 1.2.1)
```mvn clean package```

Build for a specific hadoop v1 version
```mvn -Dhadoop-one.version=1.1.2 clean package```

##### Build hadoop v2 (yarn)

Build the yarn using the default version defined in the pom
```mvn -Dhadoop.profile=2 clean package```

Build for a specific hadoop v1 version
```mvn -Dhadoop.profile=2 -Dhadoop-two.version=2.1.0-beta clean package```

It is necessary to generate separate POMs if you want to deploy to your local repository (`mvn install`) or somewhere else.
We have a script in `/tools` that generates POMs for the profiles. Use 
```mvn -f pom.hadoop2.xml clean install -DskipTests```
to put a POM file with the right dependencies into your local repository.


### Run your first program

TODO

### Eclipse Setup and Debugging

To contribute back to the project or develop your own jobs for Stratosphere, you need a working development environment. We use Eclipse and IntelliJ for development. Here we focus on Eclipse.

If you want to work on the Stratosphere code you will need maven support inside Eclipse. Just install the m2eclipse plugin: http://www.eclipse.org/m2e/

Import the Stratosphere source code using Maven's Import tool:
  * Select "Import" from the "File"-menu.
  * Expand "Maven" node, select "Existing Maven Projects", and click "next" button
  * Select the root directory by clicking on the "Browse" button and navigate to the top folder of the cloned Stratosphere Git repository.
  * Ensure that all projects are selected and click the "Finish" button.

Create a new Eclipse Project that requires Stratosphere in its Build Path!

Use this skeleton as an entry point for your own Jobs: It allows you to hit the “Run as” -> “Java Application” feature of Eclipse. (You have to stop the application manually, because only one instance can run at a time)


## Support
Don’t hesitate to ask!

[Open an issue](https://github.com/bjoernlohrmann/stratosphere/issues/new) on Github, if you found a bug or need any help.

## Documentation

There is the (old) [official Stratosphere Wiki](https://stratosphere.eu/wiki/doku).
It is in the progress of being migrated to the [GitHub Wiki](https://github.com/stratosphere/stratosphere/wiki/_pages)

Please make edits to the Wiki if you find inconsistencies or [Open an issue](https://github.com/stratosphere/stratosphere/issues/new) 


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it. 
Contact us if you are looking for implementation tasks that fit your skills.

We use the GitHub Pull Request system for the development of Stratosphere. Just open a request if you want to contribute.

### What to contribute
* Bug reports
* Bug fixes
* Documentation
* Tools that ease the use and development of Stratosphere
* Well-written Stratosphere jobs


Let us know if you have created a system that uses Stratosphere-Streaming, so that we can link to you.

## About

[Stratosphere](http://www.stratosphere.eu) is a DFG-founded research project.
We combine cutting edge research outcomes with a stable and usable codebase.







