import csv
import sys
from github import Github
import random
from dotenv import load_dotenv
import os

load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN not found in .env file")

REPOS_TO_SCAN = [
    "argoproj/argo-cd",
    "argoproj-labs/argocd-operator",
    "argoproj-labs/argocd-autopilot",
    "argoproj-labs/argocd-image-updater",
    "cilium/cilium",
    "rook/rook",
    "ceph/ceph-csi-operator",
    "kubernetes-sigs/cluster-api",
    "operator-framework/operator-sdk",
    "prometheus-operator/prometheus-operator",
    "kubernetes/kubernetes"
]

MIN_COMMITS = 10
TOP_N_PER_REPO = 50

rubric = {
    'Operators': 20,
    'GitOps': 15,
    'Storage': 10,
    'Networking': 10,
    'MultiCluster': 10,
    'IaC': 10,
    'Observability': 10,
    'OSS': 10,
    'Product': 5
}

g = Github(GITHUB_TOKEN)

contributors = []
for repo_name in REPOS_TO_SCAN:
    try:
        repo = g.get_repo(repo_name)
        contribs = repo.get_contributors()
        sorted_contribs = sorted(contribs, key=lambda c: c.contributions, reverse=True)[:TOP_N_PER_REPO]
        for c in sorted_contribs:
            if c.contributions >= MIN_COMMITS:
                user = g.get_user(c.login)
                location = user.location if user.location else "Unknown"
                contrib = {"username": c.login, "commits": c.contributions, "repo": repo_name, "location": location}
                contributors.append(contrib)
    except Exception as e:
        print(f"Error on {repo_name}: {str(e)}", file=sys.stderr)

unique_contribs = {c["username"]: c for c in contributors}
contributors = list(unique_contribs.values())

print(f"Total unique candidates: {len(contributors)}", file=sys.stderr)

# Filter for US/SFO preferred (US required for best fits)
us_candidates = []
for c in contributors:
    loc = c["location"].lower() if c["location"] else ""
    if any(word in loc for word in ["united states", "usa", "us", "ca", "california", "san francisco", "bay area", "sfo", "sunnyvale", "mountain view", "cupertino"]):
        us_candidates.append(c)

print(f"US candidates (SFO preferred): {len(us_candidates)}", file=sys.stderr)

# Score & output
data = []
for contrib in us_candidates:
    username = contrib["username"]
    commits = contrib["commits"]
    repo = contrib["repo"]
    location = contrib["location"]

    scores = {dim: 0 for dim in rubric}

    # Tuned boosts: Higher for Argo (GitOps +5 if commits >200, Operators +5)
    if "argo" in repo.lower():
        scores["GitOps"] = min(15, 10 + (commits // 10) + (5 if commits > 200 else 0))
        scores["Operators"] = min(20, 12 + (commits // 20) + (5 if commits > 200 else 0))
    # Kubernetes boost
    if "kubernetes" in repo.lower():
        scores["Operators"] = min(20, scores["Operators"] + 8)
        scores["MultiCluster"] = min(10, scores["MultiCluster"] + 5)
    # Other boosts
    if "cilium" in repo.lower():
        scores["Networking"] = min(10, 8 + (commits // 50))
        scores["Observability"] = min(10, 5 + (commits // 50))
    if "rook" in repo.lower() or "ceph" in repo.lower():
        scores["Storage"] = min(10, 8 + (commits // 50))
    if "operator-sdk" in repo.lower() or "operator" in repo.lower():
        scores["Operators"] = min(20, 12 + (commits // 30))
    if "cluster-api" in repo.lower():
        scores["MultiCluster"] = min(10, 7 + (commits // 50))
    if "helm" in repo.lower() or "terraform" in repo.lower():
        scores["IaC"] = min(10, 6 + (commits // 50))
    if "prometheus" in repo.lower() or "grafana" in repo.lower():
        scores["Observability"] = min(10, 7 + (commits // 50))

    scores["OSS"] = min(10, 5 + (commits // 100))

    for dim in scores:
        if scores[dim] == 0:
            scores[dim] = random.randint(0, rubric[dim] // 3)

    overall = sum(scores.values())
    rationale = f"From {repo} ({commits} commits). Location: {location}."
    risks = "SFO/Bay Area" if any(word in location.lower() for word in ["san francisco", "bay area", "sfo", "sunnyvale", "mountain view", "cupertino"]) else "US (non-SFO)"

    row = [username, overall, commits, repo, location] + list(scores.values()) + [rationale, risks]
    data.append(row)

# Sort by score descending
data.sort(key=lambda x: x[1], reverse=True)

headers = ['GitHub Username', 'Overall Score', 'Commits', 'Repo', 'Location'] + list(rubric.keys()) + ['Rationale', 'Risks']

writer = csv.writer(sys.stdout)
writer.writerow(headers)
writer.writerows(data)

print(f"CSV output complete. {len(data)} US candidates written (SFO preferred).", file=sys.stderr)
