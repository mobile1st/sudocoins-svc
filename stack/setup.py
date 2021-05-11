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
        "aws-cdk.core==1.102.0",
        "aws-cdk.aws-lambda==1.102.0",
        "aws-cdk.aws-dynamodb==1.102.0",
        "aws-cdk.aws-apigatewayv2==1.102.0",
        "aws-cdk.aws-apigatewayv2-integrations==1.102.0",
        "aws-cdk.aws-apigatewayv2-authorizers==1.102.0",
        "aws-cdk.aws-cognito==1.102.0",
        "aws-cdk.aws-iam==1.102.0",
        "aws-cdk.aws-sns==1.102.0",
        "aws-cdk.aws-sns-subscriptions==1.102.0",
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
