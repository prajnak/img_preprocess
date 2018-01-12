## Setup instructions

### Install Anaconda

```
   wget http://repo.continuum.io/archive/Anaconda3-4.1.1-Linux-x86_64.sh
   bash Anaconda3–4.1.1-Linux-x86_64.sh
```

Add these lines to your `bash_profile` to use anaconda python by default
```
   export PATH=/home/ubuntu/anaconda3/bin:$PATH"
```

### Install pip dependencies
```
pip install -U boto3 botocore
```
### Configure aws credentials
Add your aws secret id and access key to `~/.aws/credentials` file. The boto package uses this file to talk to AWS


## Run instructions
```
python preprocess.py
```
