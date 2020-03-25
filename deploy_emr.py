import boto3
import configparser


#config.read(r'/home/hadoop/dl.cfg')




def deploy_to_emr(bucket, emr):
    
    cluster_id = emr.run_job_flow(
    Name='2016_i94_full_data',
    LogUri='s3://'+bucket+'/logs',
    ReleaseLabel='emr-5.29.0',
    Applications=[
        {
            'Name': 'Spark'
        },
    ],
    Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                        }
                    }
                ]
            }
        ],
    Instances={
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,

               
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 4
               
                
                
            }
            
        ],
        #'Ec2KeyName': 'mykey',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        #'Ec2SubnetId': 'subnet-04a2978b7fc0b4606',
    },
    Steps=[
            {
                'Name': 'Setup Debugging',   
                        #'ActionOnFailure': 'CONTINUE',
                        'ActionOnFailure': 'TERMINATE_CLUSTER',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['state-pusher-script']
                        }
            },
            {
                'Name': 'Setup - copy etl.py',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    #'Args': ['aws', 's3', 'cp', 's3://ferrarisf50/elt.py', '/home/hadoop/']
                    'Args': ['aws', 's3', 'cp', 's3://'+bucket+'/i94/scr', '/home/hadoop/',
                            '--recursive']
                }
            },
#            {
#                'Name': 'Setup - copy dl.cfg',
#                'ActionOnFailure': 'CANCEL_AND_WAIT',
#                'HadoopJarStep': {
#                    'Jar': 'command-runner.jar',
#                    'Args': ['aws', 's3', 'cp', 's3://ferrarisf50/dl.cfg', '/home/hadoop/',
#                             '--recursive']
#               }
#            },
            {
                'Name': 'Run Spark',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit','--packages', 'saurfang:spark-sas7bdat:2.0.0-s_2.10', 
                            
                             '/home/hadoop/etl.py'
                             #,config['DATALAKE']['INPUT_DATA'], config['DATALAKE']['OUTPUT_DATA']
                            ]
                }
            }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
    #ServiceRole='MyEmrRole'
    
    )

    print ('cluster created with the step...', cluster_id['JobFlowId'])
    

    
"""
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': {
                                'VolumeType': 'gp2',
                                #'Iops': 100,
                                'SizeInGB': 50
                            },
                            'VolumesPerInstance': 1
                        },
                    ],
                    'EbsOptimized': True
                }                
"""
    
def main():
    config = configparser.ConfigParser()
    config.read(r'dl.cfg')
    
    bucket = config.get('S3','bucket')
    
    emr = boto3.client('emr',
                  region_name = 'us-east-1',
                 aws_access_key_id = config.get('AWS','AWS_ACCESS_KEY_ID'),
                 aws_secret_access_key = config.get('AWS','AWS_SECRET_ACCESS_KEY'))
    
    deploy_to_emr(bucket, emr)
    
    
   
    
    
if __name__ == '__main__':
    main()