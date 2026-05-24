# Mac Command Reference for Claude Code & DBT

Since you're working on Mac, here's a guide to Mac-specific commands and equivalents. This will be useful when working in Claude Code terminal sessions.

---

## Terminal Basics on Mac

### Default Shell

**Modern Mac (11+): zsh**
```bash
# Check your shell
echo $SHELL  # Should output: /bin/zsh

# Edit config file
nano ~/.zshrc    # zsh configuration
```

**Older Mac (10.14 and earlier): bash**
```bash
echo $SHELL      # Should output: /bin/bash
nano ~/.bash_profile
```

### Important Mac Path Differences

```bash
# Mac uses /usr/local for Homebrew installations
# Linux uses /usr/bin or /usr/local/bin

# On Mac, find Homebrew packages here:
ls /usr/local/opt/

# Add to PATH in your shell config:
export PATH="/usr/local/opt/coreutils/libexec/gnubin:$PATH"
```

---

## Common Commands: Mac vs Linux

| Task | Mac Default | Linux | Mac with GNU Tools |
|------|-------------|-------|---------------------|
| Date | `date` (BSD) | `date` (GNU) | `gdate` |
| Stream Editor | `sed` (BSD) | `sed` (GNU) | `gsed` |
| Text Processing | `awk` (BSD) | `awk` (GNU) | `gawk` |
| Find | `find` (works same) | `find` | `find` (same) |
| List Files | `ls` | `ls` | `ls` |

### Date Command Examples

**Mac native (BSD date):**
```bash
# Get today's date
date  # Output: Fri May 23 14:30:45 PDT 2026

# Get ISO format
date +"%Y-%m-%d"  # Output: 2026-05-23

# Add days (Mac specific syntax)
date -v+7d +"%Y-%m-%d"  # 7 days from today
```

**Linux equivalent (GNU date) - on Mac, use gdate:**
```bash
# Install GNU tools first
brew install coreutils

# Use gdate (GNU date)
gdate +"%Y-%m-%d"

# Add days (works like Linux)
gdate -d "+7 days" +"%Y-%m-%d"
```

### Stream Editor (sed) Examples

**Mac native (BSD sed) - requires empty string for -i:**
```bash
# Replace text in file (Mac)
sed -i '' 's/old_text/new_text/g' filename

# WITHOUT the '' it fails on Mac!
# sed -i 's/old_text/new_text/g' filename  ← This fails on Mac
```

**Linux (GNU sed) - different syntax:**
```bash
sed -i 's/old_text/new_text/g' filename  # Linux
gsed -i 's/old_text/new_text/g' filename # Mac with GNU tools
```

---

## File Management on Mac

### Essential Commands

```bash
# List files (Mac's ls works like Linux ls)
ls -la                     # Detailed list with hidden files
ls -lS                     # Sort by size
ls -lt                     # Sort by modification time

# Copy files
cp source.txt dest.txt     # Single file
cp -r source_dir dest_dir  # Recursive copy

# Move/rename
mv old_name.txt new_name.txt

# Create directories
mkdir -p dir1/dir2/dir3    # Create nested dirs

# Remove files/directories
rm filename                 # Delete file
rm -rf directory_name      # Recursive delete (CAREFUL!)

# Disk usage
du -sh *                   # Size of each item in current directory
df -h                      # Overall disk usage
```

### Finding Files (Mac specific features)

```bash
# Find files by name (works on Mac & Linux)
find . -name "*.sql" -type f

# Find files modified in last 7 days (Mac syntax differs)
find . -type f -mtime -7   # Mac AND Linux

# Find by size (Mac)
find . -size +10M          # Larger than 10MB
find . -size -1M           # Smaller than 1MB

# Search file contents (Mac)
grep -r "search_term" .    # Recursive search
grep -r "search_term" . --include="*.sql"  # Only SQL files

# Case-insensitive search
grep -ri "search_term" .
```

---

## Directory Navigation in Claude Code

```bash
# Current directory
pwd  # Print working directory
ls   # List current folder

# Change directory
cd ~/Projects/dbt-quant-analysis
cd ..                          # Parent directory
cd -                           # Last directory visited

# Create shortcut to project
# In ~/.zshrc or ~/.bash_profile, add:
alias dbt-proj="cd ~/Projects/dbt-quant-analysis"
# Then in terminal: dbt-proj

# Go to home directory
cd ~
cd $HOME
```

---

## File Permissions on Mac

```bash
# Make script executable
chmod +x scripts/run_daily_pipeline.sh

# Check permissions
ls -la scripts/

# Expected output for executable:
# -rwxr-xr-x  (read, write, execute for owner; read, execute for others)
# ^execute flag

# Make directory accessible
chmod +rx directory_name

# Remove execute permission
chmod -x scripts/backup.sh
```

---

## Text Editing in Claude Code Terminal

### View File Contents

```bash
# View entire file
cat dbt/models/staging/stg_raw_pricing_data.sql

# View first/last N lines
head -20 filename.sql      # First 20 lines
tail -20 filename.sql      # Last 20 lines

# View with line numbers
cat -n filename.sql
nl filename.sql

# View with syntax highlighting (if installed)
cat filename.sql | pygmentize  # Requires: pip install pygments
```

### Edit Files

```bash
# Edit in nano (easy for beginners)
nano filename.sql
# Then press Ctrl+X, then Y, then Enter to save

# Edit in vi/vim (powerful but steep learning curve)
vim filename.sql
# Press i to insert, ESC to exit insert mode, :wq to save

# Quick line count
wc -l filename.sql

# Quick replacement (in file)
sed -i '' 's/search_term/replace_with/g' filename.sql  # Mac syntax!
```

---

## DBT Commands on Mac

### Basic DBT Commands

```bash
# Verify Snowflake connection
dbt debug

# Parse models (check for syntax errors)
dbt parse

# Run all models
dbt run

# Run specific model
dbt run --select stg_raw_pricing_data

# Run multiple models
dbt run --select stg_* int_*

# Run with specific target (dev/prod)
dbt run --target prod

# Run with specific profiles directory
dbt run --profiles-dir ./config

# Test all models
dbt test

# Test specific model
dbt test --select stg_raw_pricing_data

# Generate documentation
dbt docs generate

# Serve documentation locally
dbt docs serve  # Opens: http://localhost:8000
```

### DBT with Verbosity (Debugging)

```bash
# See full SQL sent to Snowflake
dbt run --debug

# See detailed test results
dbt test --debug

# Verbose output
dbt run -v

# Very verbose output
dbt run -vv
```

### DBT Dry Run (Parse Without Executing)

```bash
# Check syntax without running
dbt parse

# Show what would run
dbt run --dry-run

# See execution plan
dbt compile
```

---

## Bash Scripting for Mac

### Create a Bash Script

```bash
# Create script file
cat > scripts/run_daily_pipeline.sh << 'EOF'
#!/bin/bash
# Daily DBT pipeline execution script
# Run as: bash scripts/run_daily_pipeline.sh

# Set error handling
set -e  # Exit on error
set -o pipefail  # Exit if pipe command fails

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Navigate to project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

log "Starting daily pipeline..."

# Load environment variables
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Run DBT pipeline
log "Running DBT models..."
dbt run --target prod

log "Running data quality tests..."
dbt test --target prod

log "Generating documentation..."
dbt docs generate

log "Pipeline completed successfully!"

EOF

# Make executable
chmod +x scripts/run_daily_pipeline.sh
```

### Run the Script

```bash
# Run from project root
bash scripts/run_daily_pipeline.sh

# Run from anywhere (with full path)
bash ~/Projects/dbt-quant-analysis/scripts/run_daily_pipeline.sh
```

### Mac-Specific Script Tips

```bash
#!/bin/bash
# Mac-compatible script header

# Use 'date' command (Mac BSD syntax)
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')

# Or use gdate if you need GNU date features
# TIMESTAMP=$(gdate '+%Y-%m-%d_%H-%M-%S')

# Create log file with timestamp
LOG_FILE="logs/pipeline_${TIMESTAMP}.log"

# Check if file exists
if [ -f "$LOG_FILE" ]; then
    echo "Log file exists"
fi

# Run command and capture output
dbt run 2>&1 | tee -a "$LOG_FILE"

# Check exit status
if [ $? -eq 0 ]; then
    echo "Success"
else
    echo "Failed"
fi
```

---

## Environment Variables on Mac

### Set Temporarily (Current Session)

```bash
# Set single variable
export SNOWFLAKE_ACCOUNT="my_account_id"

# Use variable
echo $SNOWFLAKE_ACCOUNT

# Unset variable
unset SNOWFLAKE_ACCOUNT
```

### Set Permanently (Persistent)

**For zsh (Mac 11+):**
```bash
# Edit ~/.zshrc
nano ~/.zshrc

# Add at the end:
export SNOWFLAKE_ACCOUNT="my_account_id"
export SNOWFLAKE_USER="my_user"

# Save and reload
source ~/.zshrc
```

**For bash (older Mac):**
```bash
# Edit ~/.bash_profile
nano ~/.bash_profile

# Add at the end:
export SNOWFLAKE_ACCOUNT="my_account_id"

# Save and reload
source ~/.bash_profile
```

### Load from .env File

```bash
# In your script or shell session
set -a  # Auto-export variables
source .env
set +a  # Stop auto-exporting

# Now variables are available
echo $SNOWFLAKE_ACCOUNT
```

---

## Package Management: Homebrew on Mac

### Basic Homebrew Commands

```bash
# Update Homebrew
brew update

# Install package
brew install package_name

# Uninstall package
brew uninstall package_name

# Search for package
brew search search_term

# Show installed packages
brew list

# Show package info
brew info node

# Clean up old versions
brew cleanup

# Check for outdated packages
brew outdated
```

### Useful Packages for Data Engineering

```bash
# Already mentioned:
brew install node@18        # Node.js (for Claude Code)
brew install coreutils      # GNU tools (date, sed, awk)
brew install git            # Version control

# Also useful:
brew install python@3.10    # Python
brew install postgresql     # PostgreSQL
brew install sqlite         # SQLite
brew install jq             # JSON processor
```

---

## System Information Commands on Mac

```bash
# Check macOS version
sw_vers
# Output: ProductName: macOS, ProductVersion: 12.5, BuildVersion: 21G72

# Or shorter
system_profiler SPSoftwareDataType

# Check CPU/Memory
sysctl -n hw.ncpu           # Number of CPU cores
sysctl -n hw.memsize        # RAM in bytes
free -h                     # Memory usage (if coreutils installed)

# Check disk space
df -h                       # Disk space per volume
du -sh *                    # Size of each item in current directory

# Show PATH
echo $PATH

# Show which Python version
python3 --version
which python3

# Show which Node version
node --version
which node
```

---

## Troubleshooting: Common Mac Issues

### Issue: "Permission denied" when running script

```bash
# Cause: Script not executable
# Fix: Add execute permission
chmod +x script.sh

# Verify
ls -la script.sh
# Should show 'x' in permissions
```

### Issue: "Command not found: dbt"

```bash
# Cause: dbt not installed or not in PATH
# Fix 1: Install dbt
pip install dbt-snowflake

# Fix 2: Check PATH
which dbt
echo $PATH

# Fix 3: Install globally with npm (for dbt-core)
npm install -g dbt-core
```

### Issue: "sed: illegal option -- i" error

```bash
# Cause: Using Linux sed syntax on Mac
# Wrong: sed -i 's/old/new/g' file

# Fix: Add empty string for -i flag (Mac BSD sed)
# Right: sed -i '' 's/old/new/g' file

# Or: Install GNU sed and use gsed
brew install gnu-sed
gsed -i 's/old/new/g' file
```

### Issue: Port 8000 already in use (dbt docs serve)

```bash
# Find process using port 8000
lsof -i :8000

# Kill the process (if needed)
kill -9 PID_NUMBER

# Or use different port
dbt docs serve --port 8001
```

### Issue: Python version conflicts

```bash
# Check Python version
python --version     # May be Python 2 (deprecated)
python3 --version    # Should be Python 3

# Use python3 explicitly
python3 -m pip install dbt-snowflake

# Or create alias in ~/.zshrc:
alias python='python3'
alias pip='pip3'
```

---

## Quick Reference: File Locations on Mac

```
Project:
  ~/Projects/dbt-quant-analysis/

Config files:
  ~/.zshrc or ~/.bash_profile       (Shell configuration)
  ~/.ssh/config                      (SSH configuration)
  ~/.aws/credentials                 (AWS credentials)
  
Node.js/npm:
  /usr/local/opt/node@18/            (Node installation)
  ~/.npm/                            (npm cache)
  ~/.npmrc                           (npm configuration)

Homebrew:
  /usr/local/Cellar/                 (Installed packages)
  /usr/local/opt/                    (Symlinks to packages)
  /usr/local/Caskroom/               (Larger applications)

Logs:
  ~/Projects/dbt-quant-analysis/dbt/logs/
  ~/Library/Logs/                    (Application logs)

Temporary files:
  /tmp/                              (Temporary directory)
  ~/Library/Caches/                  (Cache files)
```

---

## Keyboard Shortcuts in Mac Terminal

```bash
# Navigation
Ctrl+A          # Start of line
Ctrl+E          # End of line
Ctrl+F          # Forward one character
Ctrl+B          # Back one character
Option+→        # Forward one word
Option+←        # Back one word

# Editing
Ctrl+U          # Delete from cursor to start of line
Ctrl+K          # Delete from cursor to end of line
Ctrl+W          # Delete previous word
Ctrl+D          # Delete character at cursor

# Control
Ctrl+C          # Interrupt current command
Ctrl+Z          # Suspend current command
Ctrl+L          # Clear screen

# History
Ctrl+R          # Search command history
Ctrl+P          # Previous command
Ctrl+N          # Next command
```

---

## Useful Aliases for Mac (Add to ~/.zshrc)

```bash
# Add these to ~/.zshrc for shortcuts

alias dbt-proj="cd ~/Projects/dbt-quant-analysis"
alias dbt-debug="dbt debug && dbt parse"
alias dbt-test-run="dbt test && dbt run"
alias dbt-docs="dbt docs generate && dbt docs serve"

alias ll="ls -lah"
alias cls="clear"
alias venv="source venv/bin/activate"  # Activate Python virtual env

# Navigation shortcuts
alias proj="cd ~/Projects"
alias dl="cd ~/Downloads"

# Useful for development
alias gitlog="git log --oneline --graph --all"
alias gitpush="git add . && git commit && git push"
```

---

## Next Steps

1. ✅ Understand Mac terminal basics
2. ✅ Install required tools (Node.js, coreutils)
3. ✅ Get comfortable with Mac-specific commands
4. ✅ Set up shortcuts/aliases in your shell config
5. ✅ Start using Claude Code with confidence

---

**Last Updated:** May 2026  
**Platform:** macOS 11+  
**Status:** Ready for Use
