#!/bin/bash
###############################
# release notes:
# Version 1.00 [20251130] initial release
###############################
version="1.00"
usage()
{
cat << EOF
#This utility copies the CDSE products to CDSE staging storage.
#IMPORTANT! First export environmental variables!!!!
export RCLONE_CONFIG_CDSE_TYPE=s3
export RCLONE_CONFIG_CDSE_ACCESS_KEY_ID=YOUR_CDSE_PUBLIC_S3_KEY
export RCLONE_CONFIG_CDSE_SECRET_ACCESS_KEY=YOUR_CDSE_PRIVATE_S3_KEY
export RCLONE_CONFIG_CDSE_REGION=default
export RCLONE_CONFIG_CDSE_ENDPOINT='https://s3.waw3-1.cloudferro.com'
export RCLONE_CONFIG_CDSE_PROVIDER='Ceph'
#
# example of usage
#
# Single file upload:
cdse_upload.sh -b CDSE-YOUR-BUCKET-NAME -l "/tmp/cdse_product.tar"
# Single upload of not public dataset (to be released at certain date):
cdse_upload.sh -i -b CDSE-YOUR-BUCKET-NAME -l "/tmp/cdse_product.tar"
#
#Batch upload of all tar files residing localy in /home/ubuntu directory in 5 parallel sessions:
find /home/ubuntu -name "*.tar" | xargs -l -P 5 bash -c 'cdse_upload.sh -b CDSE-YOUR-BUCKET-NAME -l "$0"'
#
####### Requirements for .tar file creation for multi-file products 
# 
# 1) .tar file should be named exactly as the final product in the CDSE catalogue with *.tar suffix e.g. cdse_product.tar
# 2) .tar file should contain a single folder at root level named exactly as the final product in the CDSE with *.tar suffix e.g. ./cdse_product
# 3) outside of the folder in .tar (at root level ./) the technical files should be stored e.g. *_stac.json metadata, quicklook. These files ARE NOT PART OF THE PRODUCT. 
# to create a .tar file please use
tar cf /tmp/cdse_product.tar ./
# to see the structure of the tar file 
tar tf /tmp/cdse_product.tar
# the output should look like:
./
./cdse_product_stac.json
./cdse_product/
./cdse_product/some_subfolder/
./cdse_product/some_subfolder/some_raster.tif
./cdse_product/some_metadata.xml
################################################################
OPTIONS:
   -b	   [REQUIRED] bucket name to upload to specific to a producer e.g. CDSE-YOUR-BUCKET-NAME
   -h      this message
   -i	   [OPTIONAL] flag indicating if a dataset should not be immediately published after ingestion to CDSE. Useful only for data sets to be released at specific date.
   -l      [REQUIRED] local path (i.e. file system) path to input file or a directory with CDSE product name containing product files (e.g. COGs & STAC JSON metadata) 
   -o      [OPTIONAL] shall input file in the CDSE-YOUR-BUCKET-NAME bucket in the CDSE staging storage be overwritten?
   -p      [OPTIONAL] job priority ranging 0-9. Higher priority indicates that a CDSE product will be ingested faster. Default 3.  
   -r      [OPTIONAL] product name(s) to be replaced/patched by the product to uploaded. 
		   If more than one product needs to be replaced COGs than comma-separated list of names should be provided.
   -v      cdse_upload.sh version
   -w	   [OPTIONAL] processing workflow name. Default 'cdse_upload'. Do not change this unless instructed otherwise.
EOF
}
invisible='false'
priority=3
WorkflowName="cdse_upload"
while getopts “b:l:p:r:hiovw:” OPTION; do
	case $OPTION in
		b)
			bucket=$OPTARG
			;;
		h)
			usage
			exit 0
			;;
		i)
			invisible='true'
			;;
		l)
			local_file="${OPTARG%/}"
			;;
		o)  
			overwrite=' --no-check-dest'
			;;
		p)
			priority=$OPTARG
			;;
		r)  
			rep=$OPTARG
			;;
		v)
			echo version $version
			exit 0
			;;
		w)
			WorkflowName=$OPTARG
			;;
		?)
			usage
			exit 1
			;;
	esac
done
#########################################sanity checks
#verify if jq, gdal, wget, rclone utilities are installed
if ! [[ $(which jq) && $(which gdalinfo) && $(which ogrinfo) && $(which wget) && $(which rclone) ]]; then
	echo "ERROR: jq AND gdalinfo AND wget AND rclone utilities must be installed!"
	exit 2
fi
#verify if bucket was set
if [ -z $bucket ]; then
	echo "ERROR: Bucket name must be specified!"
	exit 3
fi
#verify if local_file was set
if [ -z "$local_file" ]; then
	echo "ERROR: Local path must be specified!"
	exit 4
fi
#verify if environmental variables were set
if [ -z $RCLONE_CONFIG_CDSE_TYPE ] || [ -z $RCLONE_CONFIG_CDSE_ACCESS_KEY_ID ] || [ -z $RCLONE_CONFIG_CDSE_SECRET_ACCESS_KEY ] || [ -z $RCLONE_CONFIG_CDSE_REGION ] || [ -z $RCLONE_CONFIG_CDSE_ENDPOINT ] || [ -z $RCLONE_CONFIG_CDSE_PROVIDER ]; then
	echo "ERROR: Some environmental variables starting with RCLONE_CONFIG_CDSE_XXXXX were not set!"
	exit 5
fi
#verify if file exists in the local storage
if [ ! -f "$local_file" ] ; then
    echo "ERROR: File $local_file does not exist in the local storage!"
    exit 6
fi
#verify if file is a valid .tar
tar tf $local_file &>/dev/null
if [ $? -gt 0 ] ; then
    echo "ERROR: File $local_file does not seem to be a valid *.tar archive!"
    exit 7
fi
#verify if the product has correctly named folder
if [ $(tar tf $local_file | grep -c "./$(basename ${local_file%.*})/") -eq 0 ]; then
    echo "ERROR: TAR file does not contain the folder named '$(basename ${local_file%.*})' containing all the product files."
    exit 8
fi
#verify if the product to be uploaded is readable by gdal
for gdal_product in $(tar tf $local_file | grep -E '.tif' | grep -vE '.tif.' |  cut -c3- | sed "s|^|${local_file}/|"); do
	echo $gdal_product
	gdalinfo /vsitar/${gdal_product} > /dev/null 2>&1
	if [ $? -ne 0 ]; then
		ogrinfo /vsitar/${gdal_product} > /dev/null 2>&1
		if [ $? -ne 0 ]; then
			echo "ERROR: GDAL can not open ${gdal_product}!"
			exit 10
		fi
	fi
done
echo $local_file
#verify if json STAC file within a .tar product is valid
stac_json=$(tar tf $local_file | grep '_stac.json')
if [ -z "$stac_json" ]; then
	echo "STAC json not found inside the $local_file"
	exit 11
else
	tar -xOf $local_file $stac_json | jq . >/dev/null 2>&1
	if [ $? -ne 0 ]; then
		echo "ERROR: STAC JSON is invalid. Can not parse ${stac_json}!"
        exit 12
	fi
fi

#verify if the product exists already in the CDSE OData 
odata_product_count=$(wget -qO - 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=(startswith(Name,%27'$(basename "${local_file%.*}")'%27))' | jq '.value | length')
if [ $odata_product_count -gt 0 ]; then
	echo 'ERROR: Such product exists in the CDSE!'
	exit 13
fi
#verify if the product has not been already deleted from the CDSE
deleted_product_count=$(wget -qO - 'https://catalogue.dataspace.copernicus.eu/odata/v1/DeletedProducts?$filter=(startswith(Name,%27'$(basename "${local_file%.*}")'%27))' | jq '.value | length')
if [ $deleted_product_count -gt 0 ]; then
	echo 'ERROR: Such product has been deleted from the CDSE!'
	exit 14
fi
#verify product to replace
if [ ! -z $rep ]; then
	rep_product=$(wget -qO - 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=(startswith(Name,%27'$(basename $rep | rev | cut -f 2- -d '.' | rev)'%27))')
	if [ $(printf "$rep_product" | jq '.value|length') -gt 0 ]; then
		product_to_replace=$(printf "$rep_product" |  jq -r '.value[].Name' | paste -sd, -)
	else
		echo "ERROR: Product to be patched does not exist in the CDSE: $rep"
		exit 15
	fi
fi
#print products to be replaced
if [ ! -z $product_to_replace ]; then
	echo "INFO: Following products will be replaced in the CDSE: $product_to_replace"
fi
wget -q --spider "$RCLONE_CONFIG_CDSE_ENDPOINT" || RCLONE_CONFIG_CDSE_ENDPOINT='https://s3.waw3-2.cloudferro.com'

#extract technical attributes for S3
last_modified=$(date -u -r "$local_file" '+%Y-%m-%dT%H:%M:%SZ')
s3_path=${bucket}$(date -u --date now '+/%Y/%m/%d')
timestamp=$(date -u -d now '+%Y-%m-%dT%H:%M:%SZ')
file_size=$(du -smb --apparent-size "$local_file" | cut -f1)
md5_checksum=$(md5sum -b "$local_file" | cut -c-32)

if [ $file_size -lt 5000000000 ]; then
	multipart_flag='false'
else
	multipart_flag='true'
fi

#upload product
rclone -q copy \
--s3-disable-http2 \
--s3-no-check-bucket \
--retries=20 \
--retries-sleep=1s \
--low-level-retries=20 \
--tpslimit=5 \
--checksum \
--s3-use-multipart-uploads=$multipart_flag \
--metadata \
--metadata-set odp-priority=$priority \
--metadata-set CDSE-upload-version=$version \
--metadata-set uploaded=$timestamp \
--metadata-set WorkflowName="cdse_upload" \
--metadata-set invisible=$invisible \
--metadata-set source-s3-endpoint-url=$RCLONE_CONFIG_CDSE_ENDPOINT \
--metadata-set file-size=$file_size \
--metadata-set md5=$md5_checksum \
--metadata-set created=$last_modified \
--metadata-set s3-public-key=${RCLONE_CONFIG_CDSE_ACCESS_KEY_ID} \
--metadata-set source-s3-path='s3://'${s3_path} \
--metadata-set source-cleanup=true \
--metadata-set product-to-replace=${product_to_replace}${overwrite} "$local_file" CDSE:$s3_path
rclone_exit_code=$?
if [ $rclone_exit_code != 0 ]; then
	echo "ERROR: rclone exit code:$rclone_exit_code. Failed to upload $local_file to s3://${s3_path}"
 	exit 16
else 
	echo "SUCCESS: Uploaded $local_file to s3://${s3_path} in ${RCLONE_CONFIG_CDSE_ENDPOINT} endpoint!"
 	exit 0
fi
