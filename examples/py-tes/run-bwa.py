#!/usr/bin/env python3
"""Demonstrate task submission via py-tes for any Azure TES instance."""

import logging
from dotenv import load_dotenv
import os
from pathlib import Path
import requests
import sys
from time import sleep
from typing import Dict, Optional

import tes

LOGGER = logging.getLogger(__name__)
logging.getLogger("tes").setLevel(logging.WARNING)

# Load the .env file
load_dotenv()

def main() -> None:
    # set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # import TES instances
    _file = Path(".tes_instances")
    LOGGER.info(f"Importing TES instances from file {str(_file)}")
    try:
        TES_INSTANCES = csv_to_dict(_file=".tes_instances")
    except FileNotFoundError:
        LOGGER.critical(f"No TES instances defined. Aborting.")
        sys.exit(1)

    # list TES instances
    LOGGER.info(f"Available TES instances:")
    for idx, (key, url) in enumerate(TES_INSTANCES.items()):
        LOGGER.info(f"({idx + 1}) {key}: {url}")

    # Get output URL from environment variables
    storage_acct= os.getenv('TES_OUTPUT_STORAGE_ACCT')
    output = '/' + storage_acct + '/outputs/py-tes/H06HDADXX130110.1.ATCACGAT.20k.bam'


    # set task payload
    LOGGER.info(f"Setting task payload...")
    task = tes.Task(
        inputs=[
            tes.Input(
                name='H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq',
                path='/data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq',
                url="https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq"
            ),
            tes.Input(
                name='H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq',
                path='/data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq',
                url="https://datasettestinputs.blob.core.windows.net/dataset/seq-format-conversion/NA12878_20k/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq"
            ),
            tes.Input(
                name='Homo_sapiens_assembly38.fasta',
                path='/data//Homo_sapiens_assembly38.fasta',
                url="https://datasettestinputs.blob.core.windows.net/dataset/references/hg38/v0/Homo_sapiens_assembly38.fasta"
            )
        ],
        outputs=[
            tes.Output(
                name='H06HDADXX130110.1.ATCACGAT.20k.bam',
                path='/data/H06HDADXX130110.1.ATCACGAT.20k.bam',
                url=output
            )
        ],
        executors=[
            tes.Executor(
                image="quay.io/biocontainers/bwa:0.7.18--he4a0461_1",
                command=[
                    "/bin/sh", "-c",
                    # Generate index
                    "bwa index /data/Homo_sapiens_assembly38.fasta && "
                    # Run alignment
                    "bwa mem -t 16 /data/Homo_sapiens_assembly38.fasta /data/H06HDADXX130110.1.ATCACGAT.20k_reads_1.fastq /data/H06HDADXX130110.1.ATCACGAT.20k_reads_2.fastq > /data/H06HDADXX130110.1.ATCACGAT.20k.bam"
                ],
                workdir='/data'
            )
        ],
        resources=tes.Resources(
            cpu_cores=16,
            ram_gb=32
        )
    )

    # submit tasks
    task_ids: Dict[str, str] = {}
    for key, url in TES_INSTANCES.items():
        LOGGER.info(f"Submitting task to {key} ({url})...")
        try:
            task_id = submit_task(task=task, url=url)
        except requests.exceptions.HTTPError as exc:
            LOGGER.warning(f"FAILED: {exc}")
            continue
        task_ids[task_id] = url
        LOGGER.info(f"Task ID: {task_id}")

    # check task states periodically until all tasks finished
    task_states: Dict = dict.fromkeys(task_ids, "UNKNOWN")
    FINAL_STATES = ["COMPLETE", "EXECUTOR_ERROR", "SYSTEM_ERROR", "CANCELLED"]
    sleep_time = 5

    # Continuously check until all tasks reach a final state
    while True:
        LOGGER.info(f"Waiting for {sleep_time} seconds...")
        sleep(sleep_time)
        
        LOGGER.info(f"Checking states of all tasks...")
        for task_id, url in task_ids.items():
            LOGGER.info(f"Checking state of task '{task_id}' ({url})...")
            task_state = get_task_state(task_id=task_id, url=url)
            task_states[task_id] = task_state
            LOGGER.info(f"Task state: {task_state}")
        
        # Break the loop if all tasks are in one of the FINAL_STATES
        if all(state in FINAL_STATES for state in task_states.values()):
            LOGGER.info(f"All tasks concluded.")
            break

    LOGGER.info("Done")


def csv_to_dict(_file: str) -> Dict:
    """Create dictionary from first two fields of a CSV file.

    Any other columns are ignored.

    Args:
        _file: Path to file with associative array contents.

    Returns:
        Bash associative array contents as dictionary.
    """
    _dict: Dict = {}
    with open(_file, "r") as _f:
        for line in _f:
            line_split = line.strip().split(",", maxsplit=2)
            _dict[line_split[0]] = line_split[1]
    return _dict

def submit_task(
    task: tes.Task,
    url: str,
    timeout: int = 5,
    user: Optional[str] = os.environ.get('TES_SERVER_USER'),
    password: Optional[str] = os.environ.get('TES_SERVER_PASSWORD'),
) -> str:
    """Submit task to TES instance.

    Args:
        task: Task to submit.
        url: TES instance URL.
        timeout: Timeout in seconds.
        user: Username for authentication.
        password: Password for authentication.

    Returns:
        Identifier of submitted task.
    """
    cli = tes.HTTPClient(url, timeout=timeout, user=user, password=password)
    return cli.create_task(task=task)

def get_task_state(
    task_id: str,
    url: str,
    timeout: int = 5,
    user: Optional[str] = os.environ.get('TES_SERVER_USER'),
    password: Optional[str] = os.environ.get('TES_SERVER_PASSWORD'),
) -> str:
    """Check state of task.

    Args:
        task_id: Identifier of task.
        url: TES instance URL.
        timeout: Timeout in seconds.
        user: Username for authentication.
        password: Password for authentication.

    Returns:
        State of task.
    """
    cli = tes.HTTPClient(url, timeout=timeout, user=user, password=password)
    return cli.get_task(task_id=task_id).state

if __name__ == '__main__':
    main()
