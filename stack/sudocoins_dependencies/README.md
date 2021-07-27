## Steps:

1. download the dependency in a subfolder
   ```
   pip download requests
   ```
2. construct the lambda layer in cdk
    ```python
    self.requests_layer = _lambda.LayerVersion(
        scope,
        'RequestsLayer',
        layer_version_name='requests-layer',
        code=_lambda.Code.from_asset('sudocoins_dependencies/requests')
    )
    ```
3. attach the layer to the lambda that needs the dependency
   ```python
   function = _lambda.Function(
      scope,
      'ExampleFunction',
      function_name='ExampleFunction',
      handler='example.lambda_handler',
      layers=[resources.requests_layer]
   )
    ```