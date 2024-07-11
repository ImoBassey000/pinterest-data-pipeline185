Milestone 3
Task 1: I Created a .pem file which contain the private key which was used together with the keypair to connect to the EC2 Instance through SSH Client

Task 3: Kafka 2.12-2.8.1 and IAM MSK authentication package was install on the EC2 instance, log into my AWS console and vavigate to the IAM console, on the left hand side select the Roles section
You should see a list of roles, select the one with the following format: <your_UserId>-ec2-access-role, copy this role ARN and make a note of it, Go to the Trust relationships tab and select Edit trust policy
Click on the Add a principal button and select IAM roles as the Principal type and replace ARN with the <your_UserId>-ec2-access-role ARN you have just copied.

Modify the client.properties file, inside your kafka_folder/bin directory accordingly.
