# Byzantine Fault-Tolerant FUSE Filesystem
A Byzantine fault-tolerant filesystem implemented with [BFT-SMaRt](https://github.com/bft-smart/library),
[jnr-fuse](https://github.com/SerCeMan/jnr-fuse), and [jnr-posix](https://github.com/jnr/jnr-posix).  
The goal of this project is to create a filesystem which appears to the client
to be a mounted filesystem but is coordinating with multiple replicas beind the
scenes using the BFT-SMaRt library.

**NOTE:** Since this makes changes to the underlying filesystem on each replica, 
I recommend running replicas on a VM or non-critical system. Whichever directory 
you attach a replica to can be deleted at any time by the state transfer 
mechanism. I've tested on Ubuntu but this should work on most POSIX systems.

## Getting Started with Demo
1.) Download the repo, and cd into bft-fuse  
2.) To build the project, install gradle then run: 
```
./demo.sh build
```
3.) Start the replicas with:
```
./demo.sh server 0
./demo.sh server 1
./demo.sh server 2
./demo.sh server 3
```
These run concurrently so you'll need to either run them in separate terminals 
or & them together. Once the replicas output "Ready to process operations" you
can start the client(s).

4.) Start a client with:
```
./demo.sh client 0
```
You should now see a mounted directory at bft-fuse/clients/client0. Any changes
made in this directory will be replicated to the replica directories located in 
bft-fuse/replicas/replica*

5.) To clean up build files run:
```
./demo.sh clean
```
You'll be asked if you want to delete contents you created during the demo.

## Things to Try
Once all replicas are running and a client has connected you can use the 
filesystem by entering the mounted client directory. If you delete a file from
one of the replica folders you'll see it's still available to the client as the 
remaining replicas are still in consensus. If you kill that replica and bring it
back online it will trigger a state transfer and bring that replica back up to
date.

## Customizing Configuration
The hosts.config file contains the network configuration of the replicas. The 
default configuration is to run four replicas and all clients on the same 
machine, but this can easily be changed by editing hosts.config. I recommend 
reading the [BFT-SMaRt](https://github.com/bft-smart/library) documentation for 
this as it will be the same.

Replicas (BFTServer.java) require two arguments: the serverID, which corresponds 
to the serverID provided in hosts.config, and the replicaPath, which is the 
directory where a replica will store its files. The demo.sh script sets 
replicaPath to be bft-fuse/replicas/replica* but you can change this to anywhere 
you want as long as it's initially empty or contains the exact same contents as 
the other replicas. Don't set this to anywhere critical, like your root 
directory, as the protocol can periodically delete files in this directory when
a state transfer is triggered.

Clients (BFTFuse.java) require two arguments: the clientID, for the BFT-SMaRt 
protocol, and the mountPath, which is the directory the FUSE filesystem will be
mounted to. The demo.sh script sets mountPath to be bft-fuse/clients/client* but 
you can set this to any empty directory.

## Side Notes
There are currently 14 filesystem operations implemented, enough to have a 
decent working filesystem, but I hope to add more in the future.


