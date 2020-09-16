#!/usr/bin/env python3
import logging as log
from pathlib import Path

from Pegasus.api import *

log.basicConfig(level=log.DEBUG)

# --- Site Catalog -------------------------------------------------------------
staging = Site("staging", arch=Arch.X86_64, os_type=OS.LINUX)\
            .add_directories(
                Directory(type=Directory.SHARED_SCRATCH, path="/rynge@osgconnect/ryantanaka")
                    .add_file_servers(
                        FileServer(
                            url="s3://rynge@osgconnect/ryantanaka", 
                            operation_type=Operation.ALL
                        )
                    )
            )

# did not add condor.+WantsStashCache=True because not using stashcp
condorpool = Site("condorpool", arch=Arch.X86_64, os_type=OS.LINUX)\
                .add_profiles(
                    Namespace.CONDOR,
                    style="condor", 
                    universe="vanilla",
                    request_cpus="1",
                    request_memory="1 GB",
                    request_disk="1 GB",
                    requirements=(
                        "OSGVO_OS_STRING == 'RHEL 7'"
                        " && HAS_MODULES == True"
                        " && GLIDEIN_Site =!= 'OSG_US_ASU_DELL_M420"
                    )
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
        sites=["condorpool"],
        staging_sites={condorpool.name : staging.name}
    )\
    .wait()\
    .analyze()\
    .statistics()
except PegasusClientError as e:
    print(e.output)
