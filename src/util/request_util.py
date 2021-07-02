def get_sub_safe(event):
    """ sub is extracted from the authorizer JWT claims """
    if not event:
        return None

    return event \
        .get('requestContext', {}) \
        .get('authorizer', {}) \
        .get('jwt', {}) \
        .get('claims', {}) \
        .get('sub')


def get_ip_address_safe(event):
    """ In case of AWS Lambda proxy integrations for HTTP API Payload format version 2.0.
    If there are multiple event types that should be handled here. """
    if not event:
        return None

    return event.get('requestContext', {}).get('http', {}).get('sourceIp')
