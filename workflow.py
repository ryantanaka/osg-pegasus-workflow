#!/usr/bin/env python3
import logging as log
from pathlib import Path

from Pegasus.api import *

log.basicConfig(level=log.DEBUG)

# --- Properties ---------------------------------------------------------------
props = Properties()
props["pegasus.data.configuration"] = "nonsharedfs"
props["pegasus.monitord.encoding"] = "json"
props["pegasus.catalog.workflow.amqp.url"] = "amqp://friend:donatedata@msgs.pegasus.isi.edu:5672/prod/workflows"
props.write()

# --- Site Catalog -------------------------------------------------------------
staging = Site("staging", arch=Arch.X86_64, os_type=OS.LINUX)\
            .add_directories(
                Directory(directory_type=Directory.SHARED_SCRATCH, path="/rynge@osgconnect/ryantanaka/scratch")
                    .add_file_servers(
                        FileServer(
                            url="s3://rynge@osgconnect/ryantanaka/scratch", 
                            operation_type=Operation.ALL
                        )
                    ),
                
                Directory(directory_type=Directory.LOCAL_STORAGE, path="/rynge@osgconnect/ryantanaka/outputs")
                    .add_file_servers(
                        FileServer(
                            url="s3://rynge@osgconnect/ryantanaka/outputs",
                            operation_type=Operation.ALL
                        )
                    )
            )

# did not add condor.+WantsStashCache=True because not using stashcp
condorpool = Site("condorpool", arch=Arch.X86_64, os_type=OS.LINUX)\
                .add_profiles(Namespace.PEGASUS, style="condor")\
                .add_profiles(
                    Namespace.CONDOR,
                    universe="vanilla",
                    request_cpus="1",
                    request_memory="1 GB",
                    request_disk="1 GB",
                    requirements='HAS_SINGULARITY == True'	
                )

SiteCatalog()\
    .add_sites(staging, condorpool)\
    .write()

# --- Replica Catalog ----------------------------------------------------------
ReplicaCatalog()\
    .add_replica("local", "input_file.txt", Path(".").resolve() / "input_file.txt")\
    .write()

# --- Transformation Catalog ---------------------------------------------------
hello_world = Transformation(
                name="hello_world", 
                site="local", 
                pfn=Path(".").resolve() / "hello_world.sh", 
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                is_stageable=True
            )

TransformationCatalog()\
    .add_transformations(hello_world)\
    .write()

# --- Workflow -----------------------------------------------------------------
wf = Workflow("osg-workflow")\
        .add_jobs(
            Job(hello_world)
                .add_inputs(File("input_file.txt"))
                .add_outputs(File("output_file.txt"))
        )

try:
    wf.plan(
        submit=True,
        sites=[condorpool.name],
        staging_sites={condorpool.name : staging.name},
        output_sites=["local", staging.name]
    )\
    .wait()\
    .analyze()\
    .statistics()
except PegasusClientError as e:
    print(e.output)

