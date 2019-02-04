

# You should have launched your cluster with the following:
sudo spark-ec2/spark-ec2 -k keypair --identity-file=keypair.pem 
--region=us-west-2 --zone=us-west-2a --copy-aws-credentials 
--instance-type t2.micro --worker-instances 1 launch project-launch

# Connect to your master instance:
sudo spark-ec2/spark-ec2 -k keypair --identity-file=keypair.pem 
--region=us-west-2 --zone=us-west-2a login project-launch

# Go into your spark directory
cd spark

# Check which version of Python are already installed:
ls /usr/bin/ | grep python

# See all available python 3 packages
sudo yum list | grep python3

# Install a stable version, such as python 3.5
sudo yum install python35

# Set the PYSPARK_PYTHON variable in your isntance'spark-env.sh file
# You will need to do this step when you stop/restart the cluster 
echo 'PYSPARK_PYTHON=/usr/bin/python35' >> conf/spark-env.sh

# Launch the pyspark shell in local mode to check it is running python:
./bin/pyspark

# Exit to leave the pyspark shell
exit()

# Roll changes across worker nodes.
pssh -h conf/slaves yum -y install python35
pssh -h conf/slaves 'echo "PYSPARK_PYTHON=/usr/bin/python35" > spark/conf/spark-env.sh'

# To install boto3 (AWS SDK for python), you must first install pip3:
curl -O https://bootstrap.pypa.io/get-pip.py

python35 get-pip.py

# Install the boto3 package.
python35 /usr/bin/pip3.5 install boto3

# Repeat these steps for the worker nodes.
pssh -h conf/slaves curl -O https://bootstrap.pypa.io/get-pip.py

pssh -h conf/slaves python35 get-pip.py
 
pssh -h conf/slaves python35 /usr/bin/pip3.5 install boto3


# Check to see if boto3 has been installed
# if there are no error messages, then boto3 has been installed correctly
./bin/pyspark
import boto3


# To exit the spark shell:
exit()

# To exit the the AWS EC2 terminal:
exit

# To stop your cluster:
sudo spark-ec2/spark-ec2 -k keypair --identity-file=keypair.pem 
--region=us-west-2 --zone=us-west-2a --copy-aws-credentials stop 
project-launch

