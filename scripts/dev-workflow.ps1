#Requires -Version 5.1
<#
.SYNOPSIS
    boss-zp Dev 工作流 - 创建分支、提交代码、推送并创建 PR

.DESCRIPTION
    用法:
        .\scripts\dev-workflow.ps1 -BranchName "fix/api-bug" -CommitMsg "fix: 修复..."

    示例:
        .\scripts\dev-workflow.ps1 -BranchName "fix/extract-api" -CommitMsg "fix: 修复API响应提取逻辑" -Detail "问题：str(data)无法匹配..."
#>

param(
    [Parameter(Mandatory)]
    [string]$BranchName,

    [Parameter(Mandatory)]
    [string]$CommitMsg,

    [string]$Detail = "",

    [string]$Files = "."
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent

function Step {
    param([string]$Msg)
    Write-Host "[WORKFLOW] $Msg" -ForegroundColor Cyan
}
function Ok  { param([string]$Msg) Write-Host "[OK] $Msg" -ForegroundColor Green }
function Err{ param([string]$Msg) Write-Host "[FAIL] $Msg" -ForegroundColor Red }

Set-Location $RepoRoot

# ========== Step 1: 语法验证 ==========
Step "Step 1/4: 语法验证..."
$pyFiles = Get-ChildItem -Path . -Include "*.py" -Recurse -File | Where-Object { $_.FullName -notmatch '\\venv\\|\\env\\|\.git' }
foreach ($f in $pyFiles) {
    $null = python -m py_compile $f.FullName 2>&1
    if ($LASTEXITCODE -ne 0) {
        Err "语法错误: $($f.FullName)"; exit 1
    }
}
Ok "语法验证通过 ($($pyFiles.Count) 个 .py 文件)"

# ========== Step 2: 创建分支 ==========
Step "Step 2/4: 创建分支 '$BranchName'..."
$null = git fetch origin main 2>&1
$null = git checkout main 2>&1
$null = git pull origin main 2>&1
$null = git checkout -b $BranchName 2>&1
Ok "分支已切换: $BranchName"

# ========== Step 3: 提交代码 ==========
Step "Step 3/4: 提交代码..."
$null = git add $Files
$fullMsg = if ($Detail) { "$CommitMsg`n`n$Detail" } else { $CommitMsg }
$null = git commit -m $fullMsg 2>&1
if ($LASTEXITCODE -ne 0) {
    Err "提交失败（可能没有变更）"; exit 1
}
Ok "提交成功"

# ========== Step 4: 推送并创建 PR ==========
Step "Step 4/4: 推送并创建 PR..."
$null = git push -u origin $BranchName 2>&1
if ($LASTEXITCODE -ne 0) {
    Err "推送失败"; exit 1
}
Ok "推送成功"

# 获取仓库路径
$remoteUrl = git remote get-url origin
if ($remoteUrl -match "github\.com[:/](.+?)(?:\.git)?$") {
    $repoPath = $Matches[1]
} else {
    Err "无法从 remote URL 解析仓库路径"
    exit 1
}

# 创建 PR（使用 GitHub CLI）
$prBody = @"
## 修改内容

$CommitMsg

$Detail

---
*由 dev-workflow.ps1 自动创建*
"@

$null = gh pr create `
    --repo $repoPath `
    --title "$CommitMsg" `
    --body "$prBody" `
    --base main 2>&1

if ($LASTEXITCODE -eq 0) {
    Ok "PR 创建成功！"
    Start-Process "https://github.com/$repoPath/pulls"
} else {
    Err "PR 创建失败，请手动在 GitHub 创建"
    Write-Host "仓库: https://github.com/$repoPath"
}
