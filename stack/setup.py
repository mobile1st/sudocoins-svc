import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="sudocoins_stack",
    version="0.0.1",

    description="Sudocoins CDK Python app",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="author",

    package_dir={"": "sudocoins_stack"},
    packages=setuptools.find_packages(where="sudocoins_stack"),

    install_requires=[
        "aws-cdk.core==1.120.0",
        "aws-cdk.aws-lambda==1.120.0",
        "aws-cdk.aws-lambda-event-sources==1.120.0",
        "aws-cdk.aws-dynamodb==1.120.0",
        "aws-cdk.aws-apigatewayv2==1.120.0",
        "aws-cdk.aws-apigatewayv2-integrations==1.120.0",
        "aws-cdk.aws-apigatewayv2-authorizers==1.120.0",
        "aws-cdk.aws-cognito==1.120.0",
        "aws-cdk.aws-iam==1.120.0",
        "aws-cdk.aws-sns==1.120.0",
        "aws-cdk.aws-sns-subscriptions==1.120.0",
        "aws-cdk.aws-sqs==1.120.0",
        "aws-cdk.aws-events==1.120.0",
        "aws-cdk.aws-events-targets==1.120.0",
        "aws-cdk.aws-route53==1.120.0",
        "aws-cdk.aws-route53-targets==1.120.0",
        "aws-cdk.aws-certificatemanager==1.120.0",
        "aws-cdk.aws-s3==1.120.0",
        "aws-cdk.aws-cloudfront==1.120.0",
        "aws-cdk.aws-cloudfront_origins==1.120.0",
        "aws-cdk.aws-lambda-python==1.120.0"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
