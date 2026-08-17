param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $ExperimentArgs
)

Push-Location $PSScriptRoot
try {
    go run . @ExperimentArgs
}
finally {
    Pop-Location
}
