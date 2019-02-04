# Instructions on Installation

# create a spark directory
sudo mkdir /usr/local/spark
cd /usr/local/spark

# clone the scripts
sudo git clone -b branch-2.0 https://github.com/amplab/spark-ec2.github


# Download the relevant distribution from the Apache Spark website:
# http://spark.apache.org/downloads.html
# Choose version of spark release, package type, and download type (Apache mirror)


# extract the .tar archive to a suitable location:
sudo tar -C /usr/local/spark -xvf ~/Downloads/spark-2.3.2-bin-hadoop2.7.tgz

# Log in to AWS management console:
# https://aws.amazon.com/console/
# verify that you are logged in to the US-West (Oregon) region

# Go to EC2 dashboard and click on 'Key Pairs'
# Give the pair a name (i.e. keypair)
# Download the .pem file


# Navigate to the IAM Dashboard (under Services dropdown menu)
# Create New User
# Select 'Users', then 'Add user', and give a name to new user
# Click 'Create New Group'
# Select Administrator Access policy
# Provide a group name (i.e. project-launch)
# Click 'Next: Review'
# Download credentials.csv file 

# In terminal
# Move both the .pem and csv files to your spark directory
sudo mv ~/Downloads/keypair.pem   /usr/local/spark/keypair.pem
sudo mv ~/Downloads/credentials.csv   /usr/local/spark/credentials.csv

# Make sure the .pem file is readable by the current user.
chmod 400 "keypair.pem"

# Go into the spark directory and set the environment variables with
# your credentials information
cd spark
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY


# To install Spark 2.0 on the cluster:

sudo spark-ec2/spark-ec2 -k keypair --identity-file=keypair.pem 
--region=us-west-2 --zone=us-west-2a --copy-aws-credentials 
--instance-type t2.micro --worker-instances 1 launch project-launch


# Wait for the installation to complete. Once complete, the public URL 
# of the mater instance will be shown (default port is 8080).
# Copy URL and visit the SPark dashboard in your browser.

# To log in to your master node's AWS AMI from terminal:
sudo spark-ec2/spark-ec2 -k keypair --identity-file=keypair.pem 
--region=us-west-2 --zone=us-west-2a login project-launch

# check relevant packages have been downloaded by the spark-ec2 scripts:
ls
cd spark
ls

# You can log out of th eAWS EC2 terminal by typing 'exit'.
exit

# To stop your cluster:
sudo spark-ec2/spark-ec2 -k keypair --identity-file=keypair.pem 
--region=us-west-2 --zone=us-west-2a --copy-aws-credentials stop 
project-launch

# To restart your cluster:
sudo spark-ec2/spark-ec2 -k keypair --identity-file=keypair.pem 
--region=us-west-2 --zone=us-west-2a --copy-aws-credentials start 
project-launch
