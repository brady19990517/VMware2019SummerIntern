
---------keys table
-- id name
-- 1  aaa
-- 2  bbb
-- 3  ccc
-- 4  ddd
-- 5  eee
---------result table
--'aaa'	'bbb'	'ccc'	'ddd'	'eee'
--   1	  2	      3	      4	      5


create table VMS_TEMP6 as(
    select * from(
        select id, name from keys
    ) pivot(
        min(id) for name in ('aaa' as a,'bbb' as b,'ccc' as c,'ddd' as d,'eee' as e)
    )
);


-- what i need is a table which contains
-- needed table
-- with columns 'id' 'key' 'value'
-- then pivot the needed table

select id from keys where id >= 1000000000000 and id <= 28000000000000; -- contains all the vms keys

select keys.id, keys.name, vms.value from keys, vms where keys.id in () or 

-----------------------

select keys_id, keys_name, vms_value from 
    (select id as keys_id, name as keys_name from keys where id >= 1000000000000 and id <= 28000000000000) 
        left outer join 
    (select doc_id as vms_doc_id, key as vms_key, value as vms_value from vms where doc_id = '5ad04fd20000000000000318') 
    on keys_id = vms_key;
-----------------------
create table VMS_TEMP as(
    select * from(
        select keys_name, vms_value from 
            (select keys_id, keys_name, vms_value from 
                (select id as keys_id, name as keys_name from keys where id >= 1000000000000 and id <= 28000000000000 ORDER BY id) 
                    left outer join 
                (select doc_id as vms_doc_id, key as vms_key, value as vms_value from vms where doc_id = '5ad04fd20000000000000318') 
            on keys_id = vms_key
            ORDER BY keys_id)
    ) pivot(
        min(vms_value) for keys_name in ()
    )
);
-----------------------

COLUMN temp_in_statement new_value str_in_statement
SELECT DISTINCT 
    LISTAGG('''' || keys_name || ''' AS ' || keys_name,',')
        WITHIN GROUP (ORDER BY keys_name) AS temp_in_statement 
    FROM (select name as keys_name from keys where id >= 1000000000000 and id <= 28000000000000 ORDER BY id);
-------------------------

create table VMS_TEMP as(
    select * from(
        select keys_name, vms_value from 
            (select keys_id, keys_name, vms_value from 
                (select id as keys_id, name as keys_name from keys where id >= 1000000000000 and id <= 28000000000000 ORDER BY id) 
                    left outer join 
                (select doc_id as vms_doc_id, key as vms_key, value as vms_value from vms where doc_id = '5ad04fd20000000000000318') 
            on keys_id = vms_key
            ORDER BY keys_id)
    ) pivot (
        min(vms_value) for keys_name in (
'vms.cluster' as VMS_CLUSTER,
'vms.parent' as VMS_PARENT,
'vms.pod' as VMS_POD,
'vms.usage' as VMS_USAGE,
'vms.usage.mem_provisioned_mb' as VMS_USAGE_MEM_PROVISIONED_MB,
'vms.usage.mem_usage_mb' as VMS_USAGE_MEM_USAGE_MB,
'vms.usage.datastore_usage_mb' as VMS_USAGE_DATASTORE_USAGE_MB,
'vms.usage.cpu_provisioned_num' as VMS_USAGE_CPU_PROVISIONED_NUM,
'vms.usage.mem_reserved_mb' as VMS_USAGE_MEM_RESERVED_MB,
'vms.usage.cpu_reserved_mhz' as VMS_USAGE_CPU_RESERVED_MHZ,
'vms.usage.cpu_usage_mhz' as VMS_USAGE_CPU_USAGE_MHZ,
'vms.created_at' as VMS_CREATED_AT,
'vms.storage' as VMS_STORAGE,
'vms.storage.Datastore2' as VMS_STORAGE_DATASTORE2,
'vms.storage.datastore1 (9)' as VMS_STORAGE_DATASTORE1,
'vms.type' as VMS_TYPE,
'vms.resource_pool' as VMS_RESOURCE_POOL,
'vms.uuid' as VMS_UUID,
'vms.moid' as VMS_MOID,
'vms.backing_folder' as VMS_BACKING_FOLDER,
'vms.powerState' as VMS_POWERSTATE,
'vms.updated_at' as VMS_UPDATED_AT,
'vms.vcenter' as VMS_VCENTER,
'vms.backing_vm' as VMS_BACKING_VM,
'vms.timestamp' as VMS_TIMESTAMP,
'vms.annotation' as VMS_ANNO,
'vms.annotation.createtime' as VMS_ANNO_CREATETIME,
'vms.annotation.nimbus-testbed' as VMS_ANNO_NIMBUS_TESTBED,
'vms.annotation.manager' as VMS_ANNO_MANAGER,
'vms.annotation.lease_at' as VMS_ANNO_LEASE_AT,
'vms.annotation.pod_location' as VMS_ANNO_POD_LOCATION,
'vms.annotation.lease_count' as VMS_ANNO_LEASE_COUNT,
'vms.annotation.testbed-node-name' as VMS_ANNO_TESTBED_NODE_NAME,
'vms.annotation.branch' as VMS_ANNO_BRANCH,
'vms.annotation.pod_name' as VMS_ANNO_POD_NAME,
'vms.annotation.nimbus_context' as VMS_ANNO_NIMBUS_CONTEXT,
'vms.annotation.cost_center' as VMS_ANNO_COST_CENTER,
'vms.annotation.pod_contexts' as VMS_ANNO_POD_CONTEXTS,
'vms.annotation.testbed-spec-name' as VMS_ANNO_TESTBED_SPEC_NAME,
'vms.annotation.lease' as VMS_ANNO_LEASE,
'vms.annotation.deployer' as VMS_ANNO_DEPLOYER,
'vms.annotation.deployer.vcloud-build' as VMS_ANNO_DEPLOYER_VCLOUD_BUILD,
'vms.annotation.stateless' as VMS_ANNO_STATELESS,
'vms.annotation.user' as VMS_ANNO_USER,
'vms.annotation.managers_chain_str' as VMS_ANNO_MANAGERS_CHAIN_STR,
'vms.annotation.testbed-name' as VMS_ANNO_TESTBED_NAME,
'vms.annotation.svs-branch' as VMS_ANNO_SVS_BRANCH,
'vms.annotation.svs_patch' as VMS_ANNO_SVS_PATCH,
'vms.annotation.svs_testsuite_owners' as VMS_ANNO_SVS_TESTSUITE_OWNERS,
'vms.annotation.svs_testsuite' as VMS_ANNO_SVS_TESTSUITE,
'vms.annotation.svs' as VMS_ANNO_SVS,
'vms.annotation.service' as VMS_ANNO_SERVICE,
'vms.annotation.svs_host' as VMS_ANNO_SVS_HOST,
'vms.annotation.cat_branch' as VMS_ANNO_CAT_BRANCH,
'vms.annotation.cat_viewtype_id' as VMS_ANNO_CAT_VIEWTYPE_ID,
'vms.annotation.cat_testrun' as VMS_ANNO_CAT_TESTRUN,
'vms.annotation.cat_site' as VMS_ANNO_CAT_SITE,
'vms.annotation.cat_slatype' as VMS_ANNO_CAT_SLATYPE,
'vms.annotation.cat_workload' as VMS_ANNO_CAT_WORKLOAD,
'vms.annotation.cat_site_id' as VMS_ANNO_CAT_SITE_ID,
'vms.annotation.cat_viewtype' as VMS_ANNO_CAT_VIEWTYPE,
'vms.annotation.cat_area_id' as VMS_ANNO_CAT_AREA_ID,
'vms.annotation.cat_slatype_id' as VMS_ANNO_CAT_SLATYPE_ID,
'vms.annotation.cat_area' as VMS_ANNO_CAT_AREA,
'vms.annotation.keepVMsOnFailure' as VMS_ANNO_KEEPVMSONFAILURE,
'vms.annotation.vcva-build' as VMS_ANNO_VCVA_BUILD,
'vms.annotation.template_type' as VMS_ANNO_TEMPLATE_TYPE,
'vms.annotation.template' as VMS_ANNO_TEMPLATE,
'vms.annotation.template_name' as VMS_ANNO_TEMPLATE_NAME,
'vms.annotation.nsx_network' as VMS_ANNO_NSX_NETWORK,
'vms.annotation.gateway_pod' as VMS_ANNO_GATEWAY_POD,
'vms.annotation.gateway' as VMS_ANNO_GATEWAY,
'vms.annotation.build' as VMS_ANNO_BUILD,
'vms.annotation.vmtree' as VMS_ANNO_VMTREE,
'vms.annotation.dynamicWorker' as VMS_ANNO_DYNAMICWORKER,
'vms.annotation.test-vpx' as VMS_ANNO_TEST_VPX,
'vms.annotation.tags' as VMS_ANNO_TAGS,
'vms.annotation.ip' as VMS_ANNO_IP,
'vms.annotation.vsm-build' as VMS_ANNO_VSM_BUILD,
'vms.annotation.lease_added_by_reaper' as VMS_ANNO_LEASE_AB_REAPER,
'vms.annotation.lease_added_by_storm_reaper' as VMS_ANNO_LEASE_AB_STORM_REAPER,
'vms.annotation.leased_at' as VMS_ANNO_LEASED_AT,
'vms.annotation.xvc_vmotion' as VMS_ANNO_XVC_VMOTION,
'vms.annotation.ldap_added_by_storm_reaper' as VMS_ANNO_LDAP_AB_STORM_REAPER,
'vms.annotation.mtree' as VMS_ANNO_MTREE,
'vms.annotation.manager_uid' as VMS_ANNO_MANAGER_UID,
'vms.annotation.nag-user' as VMS_ANNO_NAG_USER,
'vms.annotation.notes' as VMS_ANNO_NOTES,
'vms.annotation.DVM' as VMS_ANNO_DVM,
'vms.annotation.page' as VMS_ANNO_PAGE,
'vms.annotation.modifier' as VMS_ANNO_MODIFIER,
'vms.annotation.updates' as VMS_ANNO_UPDATES,
'vms.annotation.cat_side_id' as VMS_ANNO_CAT_SIDE_ID,
'vms.annotation.For label' as VMS_ANNO_FOR_LABEL,
'vms.annotation.test' as VMS_ANNO_TEST,
'vms.annotation.---user' as VMS_ANNO__USER,
'vms.annotation.name' as VMS_ANNO_NAME,
'vms.annotation.alps_testbed' as VMS_ANNO_ALPS_TESTBED,
'vms.annotation.alps_launcher' as VMS_ANNO_ALPS_LAUNCHER,
'vms.annotation.alps_crs_job' as VMS_ANNO_ALPS_CRS_JOB,
'vms.annotation.alps_host1_name' as VMS_ANNO_ALPS_HOST1_NAME,
'vms.annotation.alps_vc1_build' as VMS_ANNO_ALPS_VC1_BUILD,
'vms.annotation.alps_vc1_name' as VMS_ANNO_ALPS_VC1_NAME,
'vms.annotation.alps_host1_build' as VMS_ANNO_ALPS_HOST1_BUILD,
'vms.annotation.alps_host1_branch' as VMS_ANNO_ALPS_HOST1_BRANCH,
'vms.annotation.alps_vc1_branch' as VMS_ANNO_ALPS_VC1_BRANCH,
'vms.annotation.jenkins_url' as VMS_ANNO_JENKINS_URL,
'vms.annotation.butler' as VMS_ANNO_BUTLER,
'vms.annotation.alps_host3_branch' as VMS_ANNO_ALPS_HOST3_BRANCH,
'vms.annotation.alps_host2_branch' as VMS_ANNO_ALPS_HOST2_BRANCH,
'vms.annotation.description' as VMS_ANNO_DESCRIPTION,
'vms.annotation.executor' as VMS_ANNO_EXECUTOR,
'vms.annotation.hostname' as VMS_ANNO_HOSTNAME,
'vms.annotation.serviceName' as VMS_ANNO_SERVICENAME,
'vms.annotation.service_name' as VMS_ANNO_SERVICE_NAME,
'vms.annotation.user ' as VMS_ANNO_USER_,
'vms.annotation.''testbed-name' as VMS_ANNO_TESTBED_NAMEb,
'vms.annotation.product' as VMS_ANNO_PRODUCT,
'vms.annotation.butler_service_url' as VMS_ANNO_BS_URL,
'vms.annotation.butler_service_description' as VMS_ANNO_BS_DESCRIPTION,
'vms.annotation.butler_service_name' as VMS_ANNO_BS_NAME,
'vms.annotation.butler_service_executor' as VMS_ANNO_BS_EXECUTOR,
'vms.annotation.butler_service_type' as VMS_ANNO_BS_TYPE,
'vms.annotation.butler_service_template' as VMS_ANNO_BS_TEMPLATE,
'vms.annotation.framework' as VMS_ANNO_FRAMEWORK,
'vms.annotation.custom_note' as VMS_ANNO_CUSTOM_NOTE,
'vms.annotation.password' as VMS_ANNO_PASSWORD,
'vms.annotation.username' as VMS_ANNO_USERNAME,
'vms.annotation.testbedID' as VMS_ANNO_TESTBEDID,
'vms.annotation.cat' as VMS_ANNO_CAT,
'vms.annotation.no-vsan' as VMS_ANNO_NO_VSAN,
'vms.annotation.foo' as VMS_ANNO_FOO,
'vms.annotation.foo1' as VMS_ANNO_FOO1,
'vms.annotation.foo2' as VMS_ANNO_FOO2,
'vms.annotation.test_instance_id' as VMS_ANNO_TEST_INSTANCE_ID,
'vms.annotation.test_function_area' as VMS_ANNO_TEST_FUNCTION_AREA,
'vms.annotation.test_set_name' as VMS_ANNO_TEST_SET_NAME,
'vms.annotation.test_set_id' as VMS_ANNO_TEST_SET_ID,
'vms.annotation.test_instance_name' as VMS_ANNO_TEST_INSTANCE_NAME,
'vms.annotation.test_component' as VMS_ANNO_TEST_COMPO,
'vms.annotation.quota-project' as VMS_ANNO_QUOTA_PROJECT,
'vms.annotation.test_set' as VMS_ANNO_TEST_SET,
'vms.annotation.test_type' as VMS_ANNO_TEST_TYPE,
'vms.annotation.test_component=vmcrypt' as VMS_ANNO_TEST_COMPO_VMCRYPT,
'vms.annotation.test_componentNVM-VMLifeCycle' as VMS_ANNO_TEST_COMPONVM_VMLC,
'vms.annotation.jenkins-id' as VMS_ANNO_JENKINS_ID,
'vms.annotation.[foo' as VMS_ANNO_FOOb,
'vms.annotation.[foo2' as VMS_ANNO_FOO2b,
'vms.annotation.test_service' as VMS_ANNO_TEST_SERVICE,
'vms.annotation.production' as VMS_ANNO_PRODUCTION,
'vms.annotation.quota-rc' as VMS_ANNO_QUOTA_RC,
'vms.annotation.''quota-project' as VMS_ANNO_QUOTA_PROJECTb,
'vms.annotation.''product' as VMS_ANNO_PRODUCTb,
'vms.annotation.area' as VMS_ANNO_AREA,
'vms.annotation.quota_project' as VMS_ANNO_QUOTA_PROJECTc,
'vms.annotation.quota-projct' as VMS_ANNO_QUOTA_PROJCT,
'vms.annotation.cat_launchtype' as VMS_ANNO_CAT_LAUNCHTYPE,
'vms.annotation.no-nag' as VMS_ANNO_NO_NAG,
'vms.annotation.KVM host with following setup' as VMS_ANNO_KVM_HOSTWFS,
'vms.annotation.uuid' as VMS_ANNO_UUID,
'vms.annotation.influxdb_username' as VMS_ANNO_INFLUXDB_USERNAME,
'vms.annotation.influxdb_password' as VMS_ANNO_INFLUXDB_PASSWORD,
'vms.annotation.concourse_worker_name' as VMS_ANNO_CONCOURSE_WORKER_NAME,
'vms.annotation.influxdb_database' as VMS_ANNO_INFLUXDB_DATABASE,
'vms.annotation.influxdb_url' as VMS_ANNO_INFLUXDB_URL,
'vms.annotation.team' as VMS_ANNO_TEAM,
'vms.annotation.tag' as VMS_ANNO_TAG,
'vms.annotation.dummy' as VMS_ANNO_DUMMY,
'vms.annotation.dcpn' as VMS_ANNO_DCPN,
'vms.annotation.disk' as VMS_ANNO_DISK,
'vms.annotation."/esx' as VMS_ANNO_ESX,
'vms.annotation./esx' as VMS_ANNO_ESXb,
'vms.annotation.lang' as VMS_ANNO_LANG,
'vms.annotation.enableCustomLocation' as VMS_ANNO_ENABLECUSTOMLOCATION,
'vms.annotation.comment' as VMS_ANNO_COMMENT,
'vms.annotation.vivi-test' as VMS_ANNO_VIVI_TEST,
'vms.annotation.‘nimbus-testbed' as VMS_ANNO_NIMBUS_TESTBEDb,
'vms.annotation.‘cost_center' as VMS_ANNO_COST_CENTERb,
'vms.annotation.‘managers_chain_str' as VMS_ANNO_MANAGERS_CHAIN_STRb,
'vms.annotation.‘manager' as VMS_ANNO_MANAGERb,
'vms.annotation.‘user' as VMS_ANNO_USERb,
'vms.annotation.4176_69product' as VMS_ANNO_4176_69PRODUCT,
'vms.annotation.runType' as VMS_ANNO_RUNTYPE,
'vms.annotation.testSuite' as VMS_ANNO_TESTSUITE,
'vms.annotation.projectPath' as VMS_ANNO_PROJECTPATH,
'vms.annotation.changelist' as VMS_ANNO_CHANGELIST,
'vms.annotation.destResourcePool' as VMS_ANNO_DESTRESOURCEPOOL,
'vms.annotation.vmHomeProfile' as VMS_ANNO_VMHOMEPF,
'vms.annotation.vdiskSizeMap' as VMS_ANNO_VDISKSIZEMAP,
'vms.annotation.vdiskSizeMap.Hard disk 2' as VMS_ANNO_VDISKSIZEMAP_HD2,
'vms.annotation.vdiskSizeMap.Hard disk 1' as VMS_ANNO_VDISKSIZEMAP_HD1,
'vms.annotation.numVnic' as VMS_ANNO_NUMVNIC,
'vms.annotation.vnic' as VMS_ANNO_VNIC,
'vms.annotation.numVdisk' as VMS_ANNO_NUMVDISK,
'vms.annotation.fileShareToProfileMap' as VMS_ANNO_FSTPM,
'vms.annotation.fileShareToProfileMap.profile1-st-share-1' as VMS_ANNO_FSTPM_PF1_ST_SHARE_1,
'vms.annotation.fileShareToProfileMap.profile2-st-share-2' as VMS_ANNO_FSTPM_PF2_ST_SHARE_2,
'vms.annotation.destHostname' as VMS_ANNO_DESTHOSTNAME,
'vms.annotation.vdiskToProfileMap' as VMS_ANNO_VDISKTOPFMAP,
'vms.annotation.vdiskToProfileMap.Hard disk 2' as VMS_ANNO_VDISKTOPFMAP_HD2,
'vms.annotation.vdiskToProfileMap.Hard disk 1' as VMS_ANNO_VDISKTOPFMAP_HD1,
'vms.annotation.memoryInMB' as VMS_ANNO_MEMORYINMB,
'vms.annotation.memoryReservationLockedToMax' as VMS_ANNO_MRLTM,
'vms.annotation.numCpu' as VMS_ANNO_NUMCPU,
'vms.annotation.destDatastore' as VMS_ANNO_DESTDATASTORE,
'vms.annotation.svs_location' as VMS_ANNO_SVS_LOCATION,
'vms.annotation.nimbus_workload_priority' as VMS_ANNO_NIMBUS_WORKLOAD_PRIO,
'vms.vcenter_uuid' as VMS_VCENTER_UUID,
'vms.lease_ends_at' as VMS_LEASE_ENDS_AT,
'vms.raw-annotation' as VMS_RAW_ANNO,
'vms.name' as VMS_NAME,
'vms.instanceUuid' as VMS_INSTANCEUUID,
'vms.guest' as VMS_GUEST,
'vms.guest.hostName' as VMS_GUEST_HOSTNAME,
'vms.guest.ipAddress' as VMS_GUEST_IPADDRESS,
'vms.guest.nics' as VMS_GUEST_NICS,
'vms.user' as VMS_USER,
'vms.host_name' as VMS_HOST_NAME,
'vms.destroyed_at' as VMS_DESTROYED_AT,
'vms.memberOfVApp' as VMS_MEMBEROFVAPP
)
    )
);