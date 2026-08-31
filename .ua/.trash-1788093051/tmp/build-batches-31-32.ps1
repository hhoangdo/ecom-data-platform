$root = 'C:/Users/hhoangdo/Documents/Personal Projects/FSDS/coursework'
$manifest = Get-Content -Raw "$root/.ua/intermediate/batches.json" | ConvertFrom-Json
foreach ($index in 31, 32) {
  $batch = $manifest.batches | Where-Object batchIndex -eq $index
  $result = Get-Content -Raw "$root/.ua/tmp/ua-file-extract-results-$index.json" | ConvertFrom-Json
  $byPath = @{}
  foreach ($entry in $result.results) { $byPath[$entry.path] = $entry }
  $nodes = [System.Collections.Generic.List[object]]::new()
  $edges = [System.Collections.Generic.List[object]]::new()
  foreach ($file in $batch.files) {
    $entry = $byPath[$file.path]
    $complexity = if ($file.sizeLines -gt 200) {'complex'} elseif ($file.sizeLines -ge 50) {'moderate'} else {'simple'}
    $type = if ($file.fileCategory -eq 'docs') {'document'} else {'file'}
    $prefix = if ($type -eq 'document') {'document'} else {'file'}
    $summary = if ($type -eq 'document') { "Documentation for the $(Split-Path $file.path -Leaf) package area." } else { "Implements the $(Split-Path $file.path -Leaf) script or module." }
    $tags = if ($file.path -match 'evidence') {@('evidence','automation','python')} elseif ($file.path -match 'qa/') {@('quality-assurance','automation','utility')} elseif ($file.path -match 'spark') {@('spark','automation','data-pipeline')} elseif ($file.path -match 'lakehouse') {@('lakehouse','utility','data-pipeline')} elseif ($file.path -match '__init__') {@('entry-point','package','python')} else {@('utility','automation','python')}
    $fileId = "$prefix`:$($file.path)"
    $nodes.Add([ordered]@{id=$fileId;type=$type;name=(Split-Path $file.path -Leaf);filePath=$file.path;summary=$summary;tags=$tags;complexity=$complexity})
    if ($null -ne $entry) {
      foreach ($fn in $entry.functions) {
        if (($fn.endLine - $fn.startLine + 1) -ge 10) {
          $fnId = "function:$($file.path):$($fn.name)"
          $fnComplexity = if (($fn.endLine - $fn.startLine + 1) -gt 80) {'complex'} elseif (($fn.endLine - $fn.startLine + 1) -ge 30) {'moderate'} else {'simple'}
          $nodes.Add([ordered]@{id=$fnId;type='function';name=$fn.name;filePath=$file.path;lineRange=@($fn.startLine,$fn.endLine);summary="Implements $($fn.name) for the $(Split-Path $file.path -Leaf) workflow.";tags=@('utility','automation','python');complexity=$fnComplexity})
          $edges.Add([ordered]@{source=$fileId;target=$fnId;type='contains';direction='forward';weight=1.0})
        }
      }
      foreach ($cls in $entry.classes) {
        if (($cls.methods.Count -ge 2) -or (($cls.endLine - $cls.startLine + 1) -ge 20)) {
          $classId = "class:$($file.path):$($cls.name)"
          $nodes.Add([ordered]@{id=$classId;type='class';name=$cls.name;filePath=$file.path;lineRange=@($cls.startLine,$cls.endLine);summary="Defines the $($cls.name) type used by the $(Split-Path $file.path -Leaf) workflow.";tags=@('data-model','utility','python');complexity='simple'})
          $edges.Add([ordered]@{source=$fileId;target=$classId;type='contains';direction='forward';weight=1.0})
        }
      }
    }
    foreach ($target in @($batch.batchImportData.($file.path))) { $edges.Add([ordered]@{source=$fileId;target="file:$target";type='imports';direction='forward';weight=0.7}) }
  }
  $parts = [Math]::Ceiling([Math]::Max($nodes.Count / 60.0, $edges.Count / 120.0))
  if ($parts -lt 1) {$parts = 1}
  $sorted = @($batch.files | Sort-Object path)
  $perPart = [Math]::Ceiling($sorted.Count / $parts)
  for ($part = 1; $part -le $parts; $part++) {
    $slice = @($sorted | Select-Object -Skip (($part - 1) * $perPart) -First $perPart)
    $paths = @{}; foreach ($file in $slice) {$paths[$file.path] = $true}
    $partNodes = @($nodes | Where-Object {$paths[$_.filePath]})
    $partIds = @{}; foreach ($node in $partNodes) {$partIds[$node.id] = $true}
    $partEdges = @($edges | Where-Object {$partIds[$_.source]})
    $name = if ($parts -eq 1) {"batch-$index.json"} else {"batch-$index-part-$part.json"}
    @{nodes=$partNodes;edges=$partEdges} | ConvertTo-Json -Depth 8 | Set-Content -NoNewline "$root/.ua/intermediate/$name"
  }
}
