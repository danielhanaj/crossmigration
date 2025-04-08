#Load Folder by path function
function Get-FolderByPath {
    <#
.SYNOPSIS Retrieve folders by giving a path
.DESCRIPTION The function will retrieve a folder by it's path.
The path can contain any type of leave (folder or datacenter).
.NOTES
Author: Luc Dekens .PARAMETER Path The path to the folder. This is a required parameter.
.PARAMETER
Path The path to the folder. This is a required parameter.
.PARAMETER
Separator The character that is used to separate the leaves in the path. The default is '/'
.EXAMPLE
PS> Get-FolderByPath -Path "Folder1/Datacenter/Folder2"
.EXAMPLE
PS> Get-FolderByPath -Path "Folder1>Folder2" -Separator '>'
#>
    param(
        [CmdletBinding()]
        [parameter(Mandatory = $true)]
        [System.String[]]${Path},
        [char]${Separator} = '/'
    )
    process {
        if ((Get-PowerCLIConfiguration).DefaultVIServerMode -eq "Multiple") {
            $vcs = $global:defaultVIServers
        }
        else {
            $vcs = $global:defaultVIServers[0]
        }
        $folders = @()
        foreach ($vc in $vcs) {
            $si = Get-View ServiceInstance -Server $vc
            $rootName = (Get-View -Id $si.Content.RootFolder -Property Name).Name
            foreach ($strPath in $Path) {
                $root = Get-Folder -Name $rootName -Server $vc -ErrorAction SilentlyContinue
                $strPath.Split($Separator) | % {
                    $root = Get-Inventory -Name $_ -Location $root -NoRecursion -Server $vc -ErrorAction SilentlyContinue
                    if ((Get-Inventory -Location $root -NoRecursion | Select -ExpandProperty Name) -contains "vm") {
                        $root = Get-Inventory -Name "vm" -Location $root -Server $vc -NoRecursion
                    }
                }
                $root | where { $_ -is [VMware.VimAutomation.ViCore.Impl.V1.Inventory.FolderImpl] } | % {
                    $folders += Get-Folder -Name $_.Name -Location $root.Parent -NoRecursion -Server $vc
                }
            }
        }
        $folders
    }
}

#Function that appliy metadata to vCloud objects
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
         SupportsShouldProcess=$true, 
        ConfirmImpact="High" 
    )] 
    param( 
        [parameter(Mandatory=$true,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)] 
            [PSObject[]]$CIObject, 
        [parameter(Mandatory=$true)]
            [String]$Key,
        [parameter(Mandatory=$true)]
            $Value,
        [ValidateSet('General','Private','ReadOnly')]
            [String]$Visibility = 'General',
        [ValidateSet('String','Number','DateTime','Boolean')]
            [String]$Type = "String"
        ) 
    Process { 
        Foreach ($Object in $CIObject) { 
            $Metadata = New-Object VMware.VimAutomation.Cloud.Views.Metadata 
            $Metadata.MetadataEntry = New-Object VMware.VimAutomation.Cloud.Views.MetadataEntry 
            
            $Metadata.MetadataEntry[0].Key = $Key

            switch($Type) {
              'String'   { $Metadata.MetadataEntry[0].TypedValue = New-Object VMware.VimAutomation.Cloud.Views.MetadataStringValue }
              'Number'   { $Metadata.MetadataEntry[0].TypedValue = New-Object VMware.VimAutomation.Cloud.Views.MetadataNumberValue }
              'DateTime' { $Metadata.MetadataEntry[0].TypedValue = New-Object VMware.VimAutomation.Cloud.Views.MetadataDateTimeValue }
              'Boolean'  { $Metadata.MetadataEntry[0].TypedValue = New-Object VMware.VimAutomation.Cloud.Views.MetadataBooleanValue }
            }

            if ($Type -eq 'DateTime' -and $Value -eq 'Now') {
                $Metadata.MetadataEntry[0].TypedValue.Value = [string](Get-Date).ToUniversalTime().GetDateTimeFormats('s')
            } else {
                $Metadata.MetadataEntry[0].TypedValue.Value = $Value
            }
            
            switch($Visibility) {
              'General'  { } #Default, don't need to change
              'Private'  { 
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
            ($Object.ExtensionData.GetMetadata()).MetadataEntry | Where {$_.Key -eq $key } | Select @{N="CIObject";E={$Object.Name}},
            @{N="Type";E={$_.TypedValue.GetType().Name}},
            @{N="Visibility";E={ if ($_.Domain.Visibility) { $_.Domain.Visibility } else { "General" }}},
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
        [parameter(Mandatory=$true,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
            [PSObject[]]$CIObject,
            $Key
        )
    Process {
        Foreach ($Object in $CIObject) {
            If ($Key) {
                ($Object.ExtensionData.GetMetadata()).MetadataEntry | Where {$_.Key -eq $key } | Select @{N="CIObject";E={$Object.Name}},
                    @{N="Type";E={$_.TypedValue.GetType().Name}},
                    @{N="Visibility";E={ if ($_.Domain.Visibility) { $_.Domain.Visibility } else { "General" }}},
                    Key -ExpandProperty TypedValue
            } Else {
                ($Object.ExtensionData.GetMetadata()).MetadataEntry | Select @{N="CIObject";E={$Object.Name}},
                    @{N="Type";E={$_.TypedValue.GetType().Name}},
                    @{N="Visibility";E={ if ($_.Domain.Visibility) { $_.Domain.Visibility } else { "General" }}},
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
         SupportsShouldProcess=$true,
        ConfirmImpact="High"
    )]
    param(
        [parameter(Mandatory=$true,ValueFromPipeline=$true,ValueFromPipelineByPropertyName=$true)]
            [PSObject[]]$CIObject,
            $Key
        )
    Process {
        $CIObject | Foreach {
            $metadataValue = ($_.ExtensionData.GetMetadata()).GetMetaDataValue($Key)
            If($metadataValue) { $metadataValue.Delete() }
        }
    }
}


# Import the CSV file
$csvPath = "c:\1\vm_migration\vms.csv"
$vms = Import-Csv -Path $csvPath

Set-PowerCLIConfiguration -Scope Session -WebOperationTimeoutSeconds 172800 -Confirm:$false

if (Get-PSSnapin VMware.VimAutomation.Core -ea 0)
{
	Write-Host "VMware.VimAutomation.Core snapin already loaded" -ForegroundColor green
}
else
{
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
Connect-VIServer -Server $sourceVC -Credential $sourceVCcredentials -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
if ($?) {
    Write-Host "Connected to source vCenter: $sourceVC" -ForegroundColor Green
} else {
    Write-Host "Failed to connect to source vCenter: $sourceVC" -ForegroundColor Red
    exit 1
}

# Connect to Destination vCenter    
Connect-VIServer -Server $destinationVC -Credential $destinationVCcredentials -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
if ($?) {
    Write-Host "Connected to destination vCenter: $destinationVC" -ForegroundColor Green
} else {
    Write-Host "Failed to connect to destination vCenter: $destinationVC" -ForegroundColor Red
    exit 1
}

# Use the token to generate an access-token, use Org 'System' for Provider login
$vcdserver = "vcd01.lab.local"
$vcdservercredentials = Get-Credential -Message "Please enter your vcd Server credentials for $vcdserver"
Connect-CIServer -Server "vcd01.lab.local" -Org 'System' -Credential $vcdservercredentials -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
<# $token = ""
$skipsslverify = $false

try {
    $uri = "https://vcd01.lab.local/oauth/provider/token"
    $body = "grant_type=refresh_token&refresh_token=$($token)"
    $headers = @{
        'Accept'      = 'application/json'
        'ContentType' = 'application/x-www-form-urlencoded'
    }
    $access_token = (Invoke-RestMethod -Method Post -Uri $uri -Headers $headers -Body $body -SkipCertificateCheck:$skipsslverify).access_token
    
    Write-Host -ForegroundColor Green ("Created access_token from token successfully")
}
catch {
    Write-Host -ForegroundColor Red ("Could not create access_token from token, response code: $($_.Exception.Response.StatusCode.value__)")
    Write-Host -ForegroundColor Red ("Status Description: $($_.Exception.Response.ReasonPhrase).")
    break
}

try {
    Connect-CIServer -Server $vcd_server -SessionId "Bearer $access_token"
    Write-Host -ForegroundColor Green ("Connected to VCD successfully")
}
catch {
    Write-Host -ForegroundColor Red ("Could not connect to VCD, response code: $($_.Exception.Response.StatusCode.value__)")
    Write-Host -ForegroundColor Red ("Status Description: $($_.Exception.Response.ReasonPhrase).")
    break
}
 #>
# Generate report filename with timestamp
$strDate = Get-Date -format G
$strDate = $strDate.Replace(":",".")
$strDate = $strDate.Replace(" ","_")
	
	
$Outfile1 = "C:\1\report_" + $strDate + ".txt"
$Outfile2 = "C:\1\report_" + $strDate + ".csv"
New-Item $Outfile1 -type file -force
$Report = @()

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
$osTypeCapital = $osType.Substring(0,1).ToUpper() + $osType.Substring(1).ToLower()
$customerNameCapital = $customerName.Substring(0,1).ToUpper() + $customerName.Substring(1).ToLower()
$OrgVDC = "$customerId-$customerNameCapital-$osTypeCapital"

    # Get the VM object from the source vCenter
    $sourceVM = Get-VM -Name $vmName -Server $sourceVC
    $sourceVMview = $sourceVM | Get-View
    $vmTotalSize = [Math]::Round($sourceVM.ProvisionedSpaceGB,0)
     

    if ($sourceVM -eq $null) {
        Write-Host "VM $vmName not found in source vCenter. Skipping..." -ForegroundColor Yellow
        continue
    }

    # Retrieve source VM's datastore and portgroup
    $sourcePortGroup = Get-VM $sourceVM | Get-VirtualPortGroup -Distributed
    
    # Validate VM network settings (Portgroup and Datastore)
    if ($sourcePortGroup.Count -gt 1) {
    Write-Warning "$vmName has multiple portgroups, skipping."
    continue
} elseif ($sourcePortGroup.Count -lt 1 -or $sourcePortGroup -eq $null) {
    Write-Warning "$vmName has no portgroups, skipping."
    continue
}

# Retrieve corresponding datastore and portgroup in the target vCenter
$targetPortGroup = Get-VirtualPortGroup -Name $sourcePortGroup -Server $destinationVC -Distributed

#retrieve corresponding tag name
$vmview = $sourceVM | get-View
$sourcetag = (Get-TagAssignment -Entity (Get-VIObjectByVIView -VIView $vmview) -Category "VMs").Tag.Name

switch ($sourcetag.ToLower()) {
    "Tag1" { $metadatakey = "Backup_1" }
    "Tag2" { $metadatakey = "Backup_2" }
    "Tag3" { $metadatakey = "Backup_3" }
    default { 
        Write-Host "Unknown tag for VM $vmName. Skipping..." -ForegroundColor Yellow
        continue
    }
}


Write-Host "Source Information:" -ForegroundColor Cyan
$sourcePortGroup.Name

    # Get the destination cluster and datastore cluster
    $destCluster = Get-Cluster -Name $destinationCluster -Server $destinationVC
    #$destDatastoreCluster = Get-DatastoreCluster -Name $datastoreClusterName -Server $destinationVC

    if ($destCluster -eq $null) {
        Write-Host "One or more destination resources not found for VM $vmName. Skipping..." -ForegroundColor Yellow
        continue
    }

# Obtain the destination datastore and a random host from the destination cluster
$destDatastore = get-datastorecluster -name $datastoreClusterName -Server $destinationVC| get-datastore | Sort-Object -Property FreeSpaceGB -Descending | Select-Object -First 1
$destHost = $destCluster | Get-VMHost | Sort-Object -Property CpuUsageMhz -Descending | Select-Object -Last 1

$destVMFolder = Get-Folder -Name $customerName -Server $destinationVC -Type VM -ErrorAction SilentlyContinue

$a = Get-Date

Write-Host $sourceVM $vmTotalSize " GB" "Start Time: " $a.ToShortTimeString() "to " $destHost.Name -ForegroundColor Blue
    
    # Perform the cross-vCenter vMotion
Write-Host "Migrating VM $vmName to $destinationCluster and folder $customerName to $destHost.Name" -ForegroundColor Green
$resultvm = Move-VM -VM $sourceVM `
    -Destination $destHost `
    -Datastore $destDatastore `
    -DiskStorageFormat 'Thin' `
    -NetworkAdapter (Get-NetworkAdapter -VM $sourceVM) `
    -PortGroup $targetPortGroup `
    -InventoryLocation $destVMFolder `
    -Confirm:$false

    # Get VM location after migration
$destvm = Get-FolderByPath -Path "DC01/$customerName" | Get-VM -Name "$vmName" -Server $destinationVC

# Obtaining destination Org-VDC name
$myOrgVDC = Get-OrgVDC -Name $OrgVDC
  
# Importing the VM into vCloud Director
$civapp = get-civapp -Name $destvm -OrgVdc $myOrgVDC -erroraction 'silentlycontinue'
If ($civapp) {
    Write-Warning "$civapp already exists, skipping."
    sleep 2
}
else { Import-civapp -VM $destvm -OrgVdc $myOrgVDC -NoCopy }

# Apply metadata on vApp 
$vapp = Get-CIVApp -Name $destvm
New-CIMetaData -type "Boolean" -key $metadatakey -Value "Yes" -CIObject $vapp
}

# Disconnect from vCenters
Disconnect-VIServer -Server $sourceVC -Confirm:$false
Disconnect-VIServer -Server $destinationVC -Confirm:$false
Disconnect-CIServer -Server $vcdserver -Confirm:$false