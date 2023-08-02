#!/usr/bin/env python3
from typing import List, Optional
import subprocess

import boto3
import questionary

import typer
from pprint import pprint


ecs_client = boto3.client("ecs")

app = typer.Typer()


def _list_clusters() -> List[str]:
    resp = ecs_client.list_clusters()
    return [arn.split("/")[-1] for arn in resp["clusterArns"]]


def _list_services(cluster: str) -> List[str]:
    resp = ecs_client.list_services(cluster=cluster)
    return [arn.split("/")[-1] for arn in resp["serviceArns"]]


def _list_tasks(cluster: str) -> List[str]:
    resp = ecs_client.list_tasks(cluster=cluster)
    return [arn.split("/")[-1] for arn in resp["taskArns"]]


def _describe_tasks(cluster: str, task_arns: List[str]):
    resp = ecs_client.describe_tasks(cluster=cluster, tasks=task_arns)
    return resp["tasks"]


def _describe_task_def(arn: str):
    resp = ecs_client.describe_task_definition(taskDefinition=arn)
    return resp["taskDefinition"]


def _ask_for_cluster() -> Optional[str]:
    clusters: List[str] = _list_clusters()
    if not clusters:
        return None

    elif len(clusters) == 1:
        return clusters[0]

    return questionary.rawselect("plz select a cluster", choices=clusters).ask()


def _ask_for_cluster_task(cluster: str) -> Optional[str]:
    tasks: List[str] = _list_tasks(cluster=cluster)
    if not tasks:
        return None

    elif len(tasks) == 1:
        return tasks[0]

    options: List[str] = [
        "{} : {}".format(
            task["taskArn"].split("/")[-1],
            ", ".join(c["name"] for c in task["containers"]),
        )
        for task in _describe_tasks(cluster=cluster, task_arns=tasks)
    ]

    return (
        questionary.rawselect("plz select a task", choices=options)
        .ask()
        .split(":")[0]
        .strip()
    )


@app.command()
def list_clusters():
    for arn in _list_clusters():
        print(arn)


@app.command()
def list_services(cluster: str = None):
    if not cluster:
        cluster = _ask_for_cluster()

    for arn in _list_services(cluster=cluster):
        print(arn)


@app.command()
def list_tasks(cluster: str = None):
    if not cluster:
        cluster = _ask_for_cluster()

    task_arns: List[str] = _list_tasks(cluster=cluster)
    for task in _describe_tasks(cluster=cluster, task_arns=task_arns):
        print("task: ", task["taskArn"].split("/")[-1])
        for c in task["containers"]:
            print(
                "{}\t{}\t{}".format(
                    c["lastStatus"], c["name"], c["containerArn"].split("/")[-1]
                )
            )
            # pprint(task["containers"][0]["name"])

        print()


@app.command()
def attach_container(
    cluster: str = None,
    task: str = None,
    container_name: str = None,
    command: str = "/bin/bash",
):
    if not cluster:
        cluster = _ask_for_cluster()

    task_arns: List[str] = _list_tasks(cluster=cluster)

    options = []
    con_arn_to_container_d_map = {}

    for task in _describe_tasks(cluster=cluster, task_arns=task_arns):
        for c in task["containers"]:
            container_arn = c["containerArn"].split("/")[-1]
            con_arn_to_container_d_map[container_arn] = c
            option: str = "{}/{}".format(c["name"], container_arn)
            options.append(option)

    option = questionary.rawselect("choose container to attach", choices=options).ask()
    if not option:
        print("no container selected")
        return

    container_arn = option.split("/")[-1]
    container_info_d = con_arn_to_container_d_map[container_arn]

    subprocess.run(
        [
            "aws",
            "ecs",
            "execute-command",
            "--cluster",
            cluster,
            "--task",
            c["taskArn"],
            "--container",
            c["name"],
            "--interactive",
            "--command",
            "/bin/bash",
        ]
    )


@app.command()
def tail_task_log(
    cluster: str = None,
    task: str = None,
):
    if not cluster:
        cluster = _ask_for_cluster()

    task_arn: str = _ask_for_cluster_task(cluster=cluster)
    if not task_arn:
        print(f"no task found in cluster {cluster}")
        return

    task_info_d: dict = _describe_tasks(cluster=cluster, task_arns=[task_arn])[0]
    task_def_arn: str = task_info_d["taskDefinitionArn"]
    task_def_d: dict = _describe_task_def(task_def_arn)

    # pprint(task_def_d)

    container_defs: List[dict] = task_def_d["containerDefinitions"]
    container_def: dict = None
    if len(container_defs) == 1:
        container_def = container_defs[0]
    else:
        container_name = questionary.rawselect(
            "plz select container to tail", choices=[c["name"] for c in container_defs]
        ).ask()

        container_def = next(c for c in container_defs if c["name"] == container_name)

    log_stream: str = container_def["logConfiguration"]["options"]["awslogs-group"]

    print("tailing log on " + log_stream)
    subprocess.run(["aws", "logs", "tail", "--follow", log_stream])


if __name__ == "__main__":
    app()
