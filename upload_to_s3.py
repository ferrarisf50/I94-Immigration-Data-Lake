import boto3
import configparser
import glob
import os

#config.read(r'/home/hadoop/dl.cfg')




def upload_to_s3(filename, s3_filename,s3,bucket):
    '''
    Update file to S3 bucket
    
    INPUT:
    filename - the file name
    s3_filename - the path and file name in S3, for example i94/etl.py
    s3 - s3 client
    bucket - the bucket name on s3
    
    OUTPUT:
    None
    
    '''
    
    print("Upload %s begins..." % s3_filename)
    
    s3.upload_file(Filename=filename,
              Bucket=bucket,
              Key=s3_filename)
    
    print("Upload %s completed." % s3_filename)
    
def main():
    config = configparser.ConfigParser()
    config.read(r'dl.cfg')
    
    bucket = config.get('S3','bucket')
    s3 = boto3.client('s3',
                  region_name = 'us-east-1',
                 aws_access_key_id = config.get('AWS','AWS_ACCESS_KEY_ID'),
                 aws_secret_access_key = config.get('AWS','AWS_SECRET_ACCESS_KEY'))
    
    
    
    """
   
    ### upload dimension tables  ####
    file_lists =['i94addr.csv','i94port.csv','i94cit_res.csv','i94mode.csv','i94visa.csv','iata_airlines.csv']
    
    for file in file_lists:
        upload_to_s3(file,'i94/data/'+file,s3,bucket)
     
    ### upload i94 immigration dataset ###
    all_files = glob.glob("../../data/18-83510-I94-Data-2016/*.sas7bdat")

    
    
    for filename in all_files:
        upload_to_s3(filename, \
                 'i94/data/'+os.path.basename(filename),  \
                 s3,          \
                 bucket)
    """
    
    ### upload etl.py and configuration file ###
    upload_to_s3('etl.py', \
                 'i94/scr/etl.py',  \
                 s3,          \
                 bucket)
    
    upload_to_s3('dl.cfg', \
                 'i94/scr/dl.cfg',  \
                 s3,          \
                 bucket)
    
    print("All uploads completed!")
    
if __name__ == '__main__':
    main()