#Function that applly metadata to vCloud objects
Function New-CIMetaData { 
    <# 
    .SYNOPSIS 
        Creates a Metadata Key/Value pair. 
    .DESCRIPTION 
        Creates a custom Metadata Key/Value pair on a specified vCloud object 
    .PARAMETER  Key 
        The name of the Metadata to be applied.
    .PARAMETER  Value
        The value of the Metadata to be applied, the string 'Now' can be used
        for the current date/time for values using the 'DateTime' type.
    .PARAMETER  Visibility
        The visibility of the Metadata entry (General, Private, ReadOnly)
    .PARAMETER  Type
        The type of the Metadata entry (String, Number, DateTime, Boolean)
        (these correspond to the types of: MetadataStringValue,
        MetadataNumberValue, MetadataDateTimeValue or MetadataBooleanValue
        respectively)
    .PARAMETER  CIObject
        The object on which to apply the Metadata.
    .EXAMPLE
        New-CIMetadata -Key "Owner" -Value "Alan Renouf" -CIObject (Get-Org Org1)
        Creates a new metadata value "Alan Renouf" in a key "Owner" on the Org1 object.
    .EXAMPLE
        New-CIMetadata -Key "Company" -Value "ABC Corp" -Visibility READONLY -CIObject (Get-CIVM 'client')
        Creates a new metadata value "ABC Corp" in a key "Company" on the 'client' VM object with the READONLY attribute set preventing changes by non-system users.
    .EXAMPLE
        New-CIMetadata -Key "Backup" -Value $false -Visibility Private -Type Boolean -CIObject (Get-CIVapp 'testvapp')
        Creates a new hidden metadata value $false in a key "Backup" on the vApp object with the 'Private' attribute set preventing visibility to non-system users.
    .NOTES
        NAME: Get-CIMetaData
        AUTHOR: Jon Waite based on code by Alan Renouf
        LASTEDIT: 2016-02-23
        KEYWORDS: metadata set vcloud director
    #Requires -Version 2.0
    #> 
    [CmdletBinding( 
        SupportsShouldProcess = $true, 
        ConfirmImpact = "High" 
    )] 
    param( 
        [parameter(Mandatory = $true, ValueFromPipeline = $true, ValueFromPipelineByPropertyName = $true)] 
        [PSObject[]]$CIObject, 
        [parameter(Mandatory = $true)]
        [String]$Key,
        [parameter(Mandatory = $true)]
        $Value,
        [ValidateSet('General', 'Private', 'ReadOnly')]
        [String]$Visibility = 'General',
        [ValidateSet('String', 'Number', 'DateTime', 'Boolean')]
        [String]$Type = "String"
    ) 
    Process { 
        Foreach ($Object in $CIObject) { 
            $Metadata = New-Object VMware.VimAutomation.Cloud.Views.Metadata 
            $Metadata.MetadataEntry = New-Object VMware.VimAutomation.Cloud.Views.MetadataEntry 
            
            $Metadata.MetadataEntry[0].Key = $Key

            switch ($Type) {
                'String' { $Metadata.MetadataEntry[0].TypedValue = New-Object VMware.VimAutomation.Cloud.Views.MetadataStringValue }
                'Number' { $Metadata.MetadataEntry[0].TypedValue = New-Object VMware.VimAutomation.Cloud.Views.MetadataNumberValue }
                'DateTime' { $Metadata.MetadataEntry[0].TypedValue = New-Object VMware.VimAutomation.Cloud.Views.MetadataDateTimeValue }
                'Boolean' { $Metadata.MetadataEntry[0].TypedValue = New-Object VMware.VimAutomation.Cloud.Views.MetadataBooleanValue }
            }

            if ($Type -eq 'DateTime' -and $Value -eq 'Now') {
                $Metadata.MetadataEntry[0].TypedValue.Value = [string](Get-Date).ToUniversalTime().GetDateTimeFormats('s')
            }
            else {
                $Metadata.MetadataEntry[0].TypedValue.Value = $Value
            }
            
            switch ($Visibility) {
                'General' { } #Default, don't need to change
                'Private' { 
                    $Metadata.MetadataEntry[0].Domain = New-Object VMware.VimAutomation.Cloud.Views.MetadataDomainTag
                    $Metadata.MetadataEntry[0].Domain.Value = 'SYSTEM'
                    $Metadata.MetadataEntry[0].Domain.Visibility = 'PRIVATE'
                }
                'ReadOnly' {
                    $Metadata.MetadataEntry[0].Domain = New-Object VMware.VimAutomation.Cloud.Views.MetadataDomainTag
                    $Metadata.MetadataEntry[0].Domain.Value = 'SYSTEM'
                    $Metadata.MetadataEntry[0].Domain.Visibility = 'READONLY'
                }      
            }

            $Object.ExtensionData.CreateMetadata($Metadata) 
            ($Object.ExtensionData.GetMetadata()).MetadataEntry | Where { $_.Key -eq $key } | Select @{N = "CIObject"; E = { $Object.Name } },
            @{N = "Type"; E = { $_.TypedValue.GetType().Name } },
            @{N = "Visibility"; E = { if ($_.Domain.Visibility) { $_.Domain.Visibility } else { "General" } } },
            Key -ExpandProperty TypedValue
        } 
    } 
} 
Function Get-CIMetaData {
    <#
    .SYNOPSIS
        Retrieves all Metadata Key/Value pairs.
    .DESCRIPTION
        Retrieves all custom Metadata Key/Value pairs on a specified vCloud object
    .PARAMETER  CIObject
        The object on which to retrieve the Metadata.
    .PARAMETER  Key
        The key to retrieve.
    .EXAMPLE
        Get-CIMetadata -CIObject (Get-Org Org1)
    #>
    param(
        [parameter(Mandatory = $true, ValueFromPipeline = $true, ValueFromPipelineByPropertyName = $true)]
        [PSObject[]]$CIObject,
        $Key
    )
    Process {
        Foreach ($Object in $CIObject) {
            If ($Key) {
                ($Object.ExtensionData.GetMetadata()).MetadataEntry | Where { $_.Key -eq $key } | Select @{N = "CIObject"; E = { $Object.Name } },
                @{N = "Type"; E = { $_.TypedValue.GetType().Name } },
                @{N = "Visibility"; E = { if ($_.Domain.Visibility) { $_.Domain.Visibility } else { "General" } } },
                Key -ExpandProperty TypedValue
            }
            Else {
                ($Object.ExtensionData.GetMetadata()).MetadataEntry | Select @{N = "CIObject"; E = { $Object.Name } },
                @{N = "Type"; E = { $_.TypedValue.GetType().Name } },
                @{N = "Visibility"; E = { if ($_.Domain.Visibility) { $_.Domain.Visibility } else { "General" } } },
                Key -ExpandProperty TypedValue
            }
        }
    }
}
Function Remove-CIMetaData {
    <#
    .SYNOPSIS
        Removes a Metadata Key/Value pair.
    .DESCRIPTION
        Removes a custom Metadata Key/Value pair on a specified vCloud object
    .PARAMETER  Key
        The name of the Metadata to be removed.
    .PARAMETER  CIObject
        The object on which to remove the Metadata.
    .EXAMPLE
        Remove-CIMetaData -CIObject (Get-Org Org1) -Key "Owner"
    #>
    [CmdletBinding(
        SupportsShouldProcess = $true,
        ConfirmImpact = "High"
    )]
    param(
        [parameter(Mandatory = $true, ValueFromPipeline = $true, ValueFromPipelineByPropertyName = $true)]
        [PSObject[]]$CIObject,
        $Key
    )
    Process {
        $CIObject | Foreach {
            $metadataValue = ($_.ExtensionData.GetMetadata()).GetMetaDataValue($Key)
            If ($metadataValue) { $metadataValue.Delete() }
        }
    }
}


# Import the CSV file
$csvPath = "c:\1\vm_migration\vms.csv"
$vms = Import-Csv -Path $csvPath

Set-PowerCLIConfiguration -Scope Session -WebOperationTimeoutSeconds 172800 -Confirm:$false

if (Get-PSSnapin VMware.VimAutomation.Core -ea 0) {
    Write-Host "VMware.VimAutomation.Core snapin already loaded" -ForegroundColor green
}
else {
    Write-Host "Loading VMware.VimAutomation.Core snapin..." -ForegroundColor Yellow
    #Add-PSSnapIn VMware.VimAutomation.Core
    set-PowerCLIConfiguration -invalidCertificateAction "ignore" -confirm:$false
}

# Source vCenter credentials
$sourceVC = "vcsa02.lab.local"
$sourceVCcredentials = Get-Credential -Message "Please enter your vCenter Server credentials for $sourceVC"


# Destination vCenter credentials
$destinationVC = "vcsa01.lab.local"
$destinationVCcredentials = Get-Credential -Message "Please enter your vCenter Server credentials for $destinationVC"


# Connect to Source vCenter
Connect-VIServer -Server $sourceVC -Credential $sourceVCcredentials -WarningAction SilentlyContinue -ErrorAction Stop
if ($?) {
    Write-Host "Connected to source vCenter: $sourceVC" -ForegroundColor Green
}
else {
    Write-Host "Failed to connect to source vCenter: $sourceVC" -ForegroundColor Red
    exit 1
}

# Connect to Destination vCenter    
Connect-VIServer -Server $destinationVC -Credential $destinationVCcredentials -WarningAction SilentlyContinue -ErrorAction Stop
if ($?) {
    Write-Host "Connected to destination vCenter: $destinationVC" -ForegroundColor Green
}
else {
    Write-Host "Failed to connect to destination vCenter: $destinationVC" -ForegroundColor Red
    exit 1
}

# Use the token to generate an access-token, use Org 'System' for Provider login
$vcdserver = "vcd01.lab.local"
$vcdservercredentials = Get-Credential -Message "Please enter your vcd Server credentials for $vcdserver"
Connect-CIServer -Server "vcd01.lab.local" -Org 'System' -Credential $vcdservercredentials -WarningAction SilentlyContinue -ErrorAction SilentlyContinue

$Report = [System.Collections.ArrayList]@()

# Loop through each VM in the CSV
foreach ($vm in $vms) {
    $vmName = $vm.vm_name
    $osType = $vm.os_type
    $datastoreClusterName = $vm.datastore_cluster_name
    $customerName = $vm.customer_name
    $customerId = $vm.customer_id

    # Determine the destination cluster based on os_type
    switch ($osType.ToLower()) {
        "windows" { $destinationCluster = "windows_cluster" }
        "linux" { $destinationCluster = "linux_cluster" }
        "sql" { $destinationCluster = "sql_cluster" }
        default { 
            Write-Host "Unknown OS Type for VM $vmName. Skipping..." -ForegroundColor Yellow
            continue
        }
    }

    #GenerateCloud Director OrgVDC name
    $osTypeCapital = $osType.Substring(0, 1).ToUpper() + $osType.Substring(1).ToLower()
    $customerNameCapital = $customerName.Substring(0, 1).ToUpper() + $customerName.Substring(1).ToLower()
    $OrgVDC = "${customerId}-${customerNameCapital}-${osTypeCapital}"
}
    # Get the VM object from the source vCenter
    $sourceVM = Get-VM -Name $vmName -Server $sourceVC -erroraction 'silentlycontinue'

    # Check if the VM exists in the source vCenter
    if ($sourceVM -eq $null) {
        Write-Host "VM $vmName not found in source vCenter. Skipping..." -ForegroundColor Yellow
        continue
    }

    # Check if the VM exists in the destination vCenter
    $destinationVM = Get-VM -Name $vmName -Server $destinationVC -ErrorAction SilentlyContinue
    if ($destinationVM -ne $null) {
        Write-Host "VM $vmName already exists in destination vCenter. Skipping migration..." -ForegroundColor Yellow
        continue
    }

    # Calculate VM disk size 
    $vmTotalSize = [Math]::Round($sourceVM.ProvisionedSpaceGB, 0)
     

    # Check if VM has an attached ISO file
    if ($sourceVM | Get-CDDrive | Where-Object { $_.IsoPath -ne $null }) {
        Write-Host "VM $($sourceVM.Name) has an attached ISO file. Disconnecting it..." -ForegroundColor Yellow
        $sourceVM | Get-CDDrive | Set-CDDrive -NoMedia -Confirm:$false -ErrorAction SilentlyContinue
    }
    else {
        Write-Host "VM $($sourceVM.Name) does not have an attached ISO file." -ForegroundColor Green
    }

    # Get source cluster and source datastore for reporting purposes
    $sourceCluster = Get-Cluster -VM $sourceVM -Server $sourceVC
    $sourceDatastore = Get-Datastore -VM $sourceVM -Server $sourceVC

    # Get the destination cluster and datastore cluster
    $destCluster = Get-Cluster -Name $destinationCluster -Server $destinationVC
  
    if ($destCluster -eq $null) {
        Write-Host "One or more destination resources not found for VM $vmName. Skipping..." -ForegroundColor Yellow
        continue
    }

    # Find ESXi host that has most available CPU MHz
    $destHost = $destCluster | Get-VMHost | Sort-Object -Property CpuUsageMhz -Descending | Select-Object -Last 1
    
    # Obtain DVswitch
    $VirtualSwitch = Get-VMHost -Name $destHost.Name | Get-VDSwitch
    $networkAdapter = Get-NetworkAdapter -VM $sourcevm -Server $sourceVC
    $networkadapters = @()
    $targetPortGroup = Get-NetworkAdapter -VM $sourcevm -Server $sourceVC | Sort-Object -Property Name |
    ForEach-Object -Process {
        $networkadapters += $_
        Get-VDPortgroup -Name $networkadapters[-1].NetworkName.Replace("|", "_") -VDSwitch $VirtualSwitch -ErrorAction Stop
    }

    #Retrieve corresponding tag name
    $sourcetag = (Get-TagAssignment $sourceVM).Tag.Name


    if ([string]::IsNullOrWhiteSpace($sourcetag)) {
        Write-Host "No tag assigned to VM $vmName. Skipping metadata assignment..." -ForegroundColor Yellow
        $metadatakey = $null
    }
    else {
        switch ($sourcetag.ToLower()) {
            "tag1" { $metadatakey = "Backup_1" }
            "tag2" { $metadatakey = "Backup_2" }
            "tag3" { $metadatakey = "Backup_3" }
            default { 
                Write-Host "Unknown tag for VM $vmName. Skipping metadata assignment..." -ForegroundColor Yellow
                $metadatakey = $null
            }
        }
    }


    Write-Host "Source Information:" -ForegroundColor Cyan

    # Obtain the destination datastore and a random host from the destination cluster
    $destDatastore = get-datastorecluster -name $datastoreClusterName -Server $destinationVC | get-datastore | Sort-Object -Property FreeSpaceGB -Descending | Select-Object -First 1

    # Check if the destination datastore has enough free space to fit migrated VM
    if ($destDatastore.FreeSpaceGB -lt $vmTotalSize) {
        Write-Host "Insufficient space in datastore '$($destDatastore.Name)' for VM $vmName. Required: $vmTotalSize GB, Available: $($destDatastore.FreeSpaceGB) GB. Skipping migration..." -ForegroundColor Red
        continue
    }

    # Obtain destination folder
    $destVMFolder = Get-Folder -Name $customerName -Server $destinationVC -Type VM -ErrorAction SilentlyContinue

    if ($destVMFolder.Count -gt 1) {
        Write-Host "Multiple folders with the name '$customerName' exist at the destination site. Skipping VM $vmName..." -ForegroundColor Red
        continue
    }
    elseif ($destVMFolder.Count -eq 0) {
        Write-Host "No folder with the name '$customerName' exists at the destination site. Skipping VM $vmName..." -ForegroundColor Yellow
        continue
    }

    $currenttime = Get-Date
    Write-Host VM $sourceVM with total size $vmTotalSize " GB" started migrating at "Start Time: " $currenttime.ToShortTimeString() "to " $destHost.Name -ForegroundColor Blue

    # Perform the cross-vCenter vMotion
    Write-Host "Migrating VM $vmName to $destinationCluster and folder $customerName to $destHost.Name" -ForegroundColor Green
    $resultvm = Move-VM -VM $sourceVM `
        -Destination $destHost `
        -Datastore $destDatastore `
        -DiskStorageFormat 'Thin' `
        -NetworkAdapter $networkAdapter `
        -PortGroup $targetPortGroup `
        -InventoryLocation $destVMFolder `
        -Confirm:$false

    While ('Running', 'Queued' -contains $resultvm.State) {

        Write-Host "...... $($resultvm.PercentComplete)%"
         
        Start-Sleep 30
         
        $resultvm = Get-Task -Id $resultvm.ID -ErrorAction Ignore
             
    }
    #Write-Host $vm.Name " " $resultvm.Name "  " $resultvm.ID " " $resultvm.State " " $resultvm.PercentComplete " Start " $resultvm.StartTime " End " $resultvm.FinishTime
    
    # Obtain VM object at destination site
    $destvm = $destvmFolder | Get-VM -Name $vmName -Server $destinationVC

    # Obtaining destination Org-VDC name
    $myOrgVDC = Get-OrgVDC -Name $OrgVDC
  
    # Importing the VM into vCloud Director. If vapp with same name aready exist, skip importing
    $civapp = get-civapp -Name $destvm -OrgVdc $myOrgVDC -erroraction 'silentlycontinue'
    If ($civapp) {
        Write-Warning "$civapp already exists on $customerid tenant in VCD, skipping."
        sleep 2
    }
    else { Import-civapp -VM $destvm -OrgVdc $myOrgVDC -NoCopy }

    # Apply metadata on vApp 
    if ($metadatakey) {
        $vapp = Get-CIVApp -Name $destvm
        New-CIMetaData -type "Boolean" -key $metadatakey -Value "Yes" -CIObject $vapp
    }
    else {
        Write-Host "No metadata key assigned for vApp $vmName. Skipping metadata assignment..." -ForegroundColor Yellow
    }<# Action when all if and elseif conditions are false #>
}

#Adding multiple network names into report
$sourceNetworks = ($targetPortGroup | Select-Object -ExpandProperty Name) -join ", "

$Report.Add([PSCustomObject]@{
        VMName          = $vmName
        SourceCluster   = $sourceCluster
        SourceDatastore = $sourceDatastore
        SourceTag       = ($sourcetag) -join ", "
        SourceNetworks  = $sourceNetworks
        VM_size_GB      = $vmTotalSize
    }) | Out-Null

$Report | Export-Csv -Path "C:\1\vm_migration_report.csv" -NoTypeInformation -Force
Write-Host "Migration report saved to C:\1\vm_migration_report.csv" -ForegroundColor Green

# Disconnect from vCenters
Disconnect-VIServer -Server $sourceVC, $destinationVC -Confirm:$false
Disconnect-CIServer -Server $vcdserver -Confirm:$false
