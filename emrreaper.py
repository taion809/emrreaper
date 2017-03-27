import click
import boto3
from datetime import datetime, timezone, timedelta


class Cluster:
    def __init__(self, cluster_id, name, state, created_at):
        self.cluster_id = cluster_id
        self.name = name
        self.state = state
        self.created_at = created_at

    @staticmethod
    def make(response: dict):
        return Cluster(
            response['Id'],
            response['Name'],
            response['Status']['State'],
            response['Status']['Timeline']['CreationDateTime']
        )


def fetch_clusters(svc):
    states = ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
    clusters = svc.list_clusters(ClusterStates=states)
    return [Cluster.make(c) for c in clusters['Clusters']]


def can_reap(svc, cluster):
    info = svc.describe_cluster(ClusterId=cluster.cluster_id)
    if info['Cluster']['TerminationProtected']:
        return False

    return True


def reap(svc, cluster):
    pass


@click.command()
@click.option("--sla", default=3, help='the max number of hours a cluster can survive.')
def run(sla):
    svc = boto3.client("emr")
    reapable = []
    for cluster in fetch_clusters(svc):
        sla_dt = datetime.now(timezone.utc) - timedelta(hours=int(sla))
        if cluster.created_at > sla_dt:
            continue

        if not can_reap(svc, cluster):
            continue

        click.echo("reaping cluster {}: \"{}\"".format(cluster.cluster_id, cluster.name))
        reapable.append(cluster.cluster_id)

    if len(reapable) > 0:
        svc.terminate_job_flows(JobFlowIds=reapable)

    click.echo("reaped {} clusters".format(len(reapable)))

if __name__ == "__main__":
    run()
