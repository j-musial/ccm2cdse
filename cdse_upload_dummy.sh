#!/bin/bash
#please replace PROVIDER_S3_PUBLIC_KEY, PROVIDER_S3_PRIVATE_KEY and CCM-<PROVIDER_NAME>-DEV with your credentials and your name 
############################### export variables
export RCLONE_CONFIG_CLMS_TYPE=s3
export RCLONE_CONFIG_CLMS_ACCESS_KEY_ID=<PROVIDER_S3_PUBLIC_KEY>
export RCLONE_CONFIG_CLMS_SECRET_ACCESS_KEY=<PROVIDER_S3_PRIVATE_KEY>
export RCLONE_CONFIG_CLMS_REGION=default
export RCLONE_CONFIG_CLMS_ENDPOINT='https://s3.waw3-1.cloudferro.com'
export RCLONE_CONFIG_CLMS_PROVIDER='Ceph'
export PATH="${HOME}/Dropbox/scripts/bash:$PATH"
############################### define variables
working_dir=/tmp
dummy_product_name='dummy_product_name'
bucket=CCM-<PROVIDER_NAME>-DEV
############################### create a dummy product with a random layer
mkdir -p $working_dir/${dummy_product_name}/${dummy_product_name}
cd $working_dir/${dummy_product_name}
gdal_create -outsize 20 20 -a_srs EPSG:4326 -a_ullr 2 50 3 49 -burn 10 ./${dummy_product_name}/dummy.tif
gdalinfo -json ./${dummy_product_name}/dummy.tif > ./dummy_stac.json #The STAC items should comply to https://github.com/eu-cdse/ccm2cdse/blob/main/cdse_generic_stac_item.json5  
tar -cf ${working_dir}/${dummy_product_name}.tar ./ 
wget -qO ./cdse_upload.sh 'https://github.com/eu-cdse/ccm2cdse/raw/refs/heads/main/cdse_upload.sh'
chmod u+x ./cdse_upload.sh
./cdse_upload.sh -b $bucket ${working_dir}/${dummy_product_name}.tar
cd $working_dir
rm -rf $working_dir/${dummy_product_name}
