#!/bin/bash
#------------------------------------------------------------------------------
#----------------
# Preserve the v2.0.x managed-loop byte boundary.

set -eu

REPO_ROOT=$(git rev-parse --show-toplevel)
TEMPLATE_URL=${TEMPLATE_URL:-https://github.com/LPFchan/repo-template.git}

TMP=$(mktemp -d /tmp/template-self-sync.XXXXXX)
stage=
trap 'rm -rf "$TMP"; [ -z "$stage" ] || rm -f "$stage"' EXIT

git clone -q --depth 1 "$TEMPLATE_URL" "$TMP/template"
SCAFFOLD="$TMP/template/scaffold"
MANIFEST="$SCAFFOLD/manifest.txt"
VERSION=$(sed -n 's/^\*\*Template version: \(.*\)\*\*/\1/p' "$SCAFFOLD/records/REPO.md" | head -1)
[ -n "$VERSION" ] || { echo "could not read Template version" >&2; exit 1; }
python3 -c 'import re,sys; v=sys.argv[1]; sys.exit(0 if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", v) and tuple(map(int,v.split("."))) >= (2,0,4) else "Unsafe template version: "+v+"; minimum is 2.0.4")' "$VERSION"

# Validate every destination before publishing any managed files or seeds.
python3 - "$REPO_ROOT" "$MANIFEST" "$SCAFFOLD/seed-manifest-v2.txt" <<'PY_MANIFEST'
import os
import sys

root = os.path.abspath(sys.argv[1])
workflow = root + '/.github/workflows'
for manifest in sys.argv[2:]:
    if not os.path.isfile(manifest):
        continue
    for line in open(manifest):
        entry = line.rstrip('\n').strip(' ')
        if not entry or entry.startswith('#'):
            continue
        if entry == 'AGENTS.md managed-section' and manifest == sys.argv[2]:
            continue
        if ' -> ' not in entry:
            sys.exit('Refusing invalid manifest entry: ' + entry)
        destination = entry.rsplit(' -> ', 1)[1]
        target = os.path.abspath(root + '/' + destination)
        if os.path.isabs(destination) or os.path.commonpath((root, target)) != root:
            sys.exit('Refusing destination outside the repository: ' + destination)
        for normalize in (os.path.abspath, os.path.realpath):
            dst, protected = normalize(target), normalize(workflow)
            if os.path.commonpath((dst, protected)) in (dst, protected):
                sys.exit('Refusing workflow destination or ancestor: ' + destination)
PY_MANIFEST

changed=0


while IFS= read -r line; do
  case "$line" in ''|\#*) continue ;; esac
  entry=$(echo "$line" | sed 's/^ *//; s/ *$//')
  case "$entry" in ''|\#*) continue ;; esac

  [ "$entry" != "AGENTS.md managed-section" ] || continue

  src=${entry%% -> *}
  dst=${entry##* -> }
  src_path="$SCAFFOLD/$src"
  dst_path="$REPO_ROOT/$dst"

  case "$src" in
    */)
      mkdir -p "$dst_path"
      if ! diff -qr "$src_path" "$dst_path" >/dev/null 2>&1; then
        rsync -a --delete "$src_path" "$dst_path"
        changed=1
      fi
      ;;
    *)
      if ! cmp -s "$src_path" "$dst_path" 2>/dev/null; then
        mkdir -p "$(dirname "$dst_path")"
        stage=$(mktemp "$dst_path.template-sync.XXXXXX")
        cp -p "$src_path" "$stage"
        mv -f "$stage" "$dst_path"
        chmod +x "$dst_path" 2>/dev/null || true
        changed=1
      fi
      ;;
  esac
done < "$MANIFEST"

splice_agents() {
  python3 - "$SCAFFOLD/AGENTS.md" "$REPO_ROOT/AGENTS.md" "$TMP/agents-merged.md" <<'PY'
import sys
import shutil

scaffold_path, repo_path, out_path = sys.argv[1:4]
END = "<!-- template-managed:end -->"
LEGACY_TAIL = "## Repo-Specific Rules"

scaffold = open(scaffold_path).read()
repo = open(repo_path).read()

if END not in scaffold:
    sys.exit("scaffold AGENTS.md is missing the template-managed:end marker")

managed = scaffold[: scaffold.index(END) + len(END)]

if END in repo:
    tail = repo[repo.index(END) + len(END) :]
elif LEGACY_TAIL in repo:
    tail = "\n\n" + repo[repo.index(LEGACY_TAIL) :]
elif "## Code Review Rules" in repo:
    idx = repo.index("## Code Review Rules")
    end = repo.index("\n## ", idx + 1) if "\n## " in repo[idx + 1 :] else len(repo)
    tail = repo[end:]
else:
    sys.exit(1)

open(out_path, "w").write(managed + tail)
shutil.copymode(repo_path, out_path)
PY
}


# Each managed file is staged beside its destination before rename, so updates
# replace the running script inode even when /tmp is on a different filesystem.
# The header padding keeps the managed-loop byte boundary compatible with the
# v2.0.x readers, whose first upgrade may still copy in place.
#
# The legacy file branch makes every copied file executable. Correct managed
# Markdown modes after that loop, including on the first legacy-script upgrade.
# Project-owned seeds and reports are outside this manifest and keep their modes.
while IFS= read -r line; do
  case "$line" in ''|\#*) continue ;; esac
  entry=$(echo "$line" | sed 's/^ *//; s/ *$//')
  case "$entry" in ''|\#*) continue ;; esac
  src=${entry%% -> *}
  dst=${entry##* -> }
  case "$src" in
    *.md)
      if [ -x "$REPO_ROOT/$dst" ]; then
        chmod a-x "$REPO_ROOT/$dst"
        changed=1
      fi
      ;;
  esac
done < "$MANIFEST"

# Install this cleanup in the tail as well: a legacy consumer reaches this
# code after its first self-update without having run the new script header.
stage=
trap 'rm -rf "$TMP"; [ -z "$stage" ] || rm -f "$stage"' EXIT

# Keep this out of the legacy manifest loop: old readers copy AGENTS in place.
# Preserve the local mode as well as the tail when publishing the completed merge.
if [ -f "$REPO_ROOT/AGENTS.md" ]; then
  if splice_agents; then
    if ! cmp -s "$TMP/agents-merged.md" "$REPO_ROOT/AGENTS.md"; then
      stage=$(mktemp "$REPO_ROOT/AGENTS.md.template-sync.XXXXXX")
      cp -p "$REPO_ROOT/AGENTS.md" "$stage"
      cat "$TMP/agents-merged.md" > "$stage"
      mv -f "$stage" "$REPO_ROOT/AGENTS.md"
      changed=1
    fi
  else
    echo "AGENTS.md has no template boundary; skipping managed-section sync" >&2
  fi
fi

# Registers belong to the adopted project once created. Keep seeds separate
# from the managed manifest so the first run of an older sync script is safe.
if [ -f "$SCAFFOLD/seed-manifest-v2.txt" ]; then
  while IFS= read -r line; do
    case "$line" in ''|\#*) continue ;; esac
    entry=$(echo "$line" | sed 's/^ *//; s/ *$//')
    case "$entry" in ''|\#*) continue ;; esac
    src=${entry%% -> *}
    dst=${entry##* -> }
    dst_path="$REPO_ROOT/$dst"
    # Also preserve empty files, directories, and dangling symbolic links.
    if [ ! -e "$dst_path" ] && [ ! -L "$dst_path" ]; then
      mkdir -p "$(dirname "$dst_path")"
      stage=$(mktemp "$dst_path.template-sync.XXXXXX")
      cp -p "$SCAFFOLD/$src" "$stage"
      # Publish a completed seed without overwriting a concurrently created
      # project file, directory, or symbolic link. Both paths share a filesystem.
      seeded=$(python3 - "$stage" "$dst_path" <<'PY_SEED'
import errno
import os
import sys

try:
    os.link(sys.argv[1], sys.argv[2])
except FileExistsError:
    print(0)
except OSError as error:
    if error.errno in {errno.ENOTSUP, errno.EOPNOTSUPP, errno.ENOSYS, errno.EPERM}:
        sys.exit(
            f"Cannot initialize {sys.argv[2]}: atomic hard-link publication is "
            f"unavailable or not permitted ({error}). The register remains absent and "
            "existing project records are preserved. Initialize the missing "
            "register manually or run from a filesystem with hard-link support."
        )
    raise
else:
    print(1)
PY_SEED
      )
      [ "$seeded" = 0 ] || changed=1
      rm -f "$stage"
      stage=
    fi
  done < "$SCAFFOLD/seed-manifest-v2.txt"
fi

# Workflow changes need separate review and permissions. Never copy them during
# automatic sync; report drift so the operator can install a reviewed update.
if ! cmp -s "$SCAFFOLD/template-sync.yml" "$REPO_ROOT/.github/workflows/template-sync.yml"; then
  echo "Workflow update available: review and update .github/workflows/template-sync.yml separately." >&2
  echo "SYNC-WORKFLOW-DRIFT=1"
else
  echo "SYNC-WORKFLOW-DRIFT=0"
fi

if [ -f "$REPO_ROOT/records/REPO.md" ]; then
  if ! grep -Fxq "**Template version: $VERSION**" "$REPO_ROOT/records/REPO.md"; then
    python3 - "$REPO_ROOT/records/REPO.md" "$VERSION" > "$TMP/repo-version.md" <<'PY_VERSION'
from pathlib import Path
import re
import sys

source = Path(sys.argv[1]).read_bytes()
updated = re.sub(rb'(?m)^\*\*Template version: [0-9.]+\*\*(?=\r?$)',
                 f'**Template version: {sys.argv[2]}**'.encode(), source)
sys.stdout.buffer.write(updated)
PY_VERSION
    if ! cmp -s "$TMP/repo-version.md" "$REPO_ROOT/records/REPO.md"; then
      stage=$(mktemp "$REPO_ROOT/records/REPO.md.template-sync.XXXXXX")
      cp -p "$REPO_ROOT/records/REPO.md" "$stage"
      cat "$TMP/repo-version.md" > "$stage"
      mv -f "$stage" "$REPO_ROOT/records/REPO.md"
      changed=1
    fi
  fi
fi

echo "SYNC-VERSION=$VERSION"
echo "SYNC-CHANGED=$changed"
