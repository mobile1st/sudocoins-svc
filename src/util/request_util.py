def get_sub_safe(event):
    """ sub is extracted from header or the authorizer JWT claims """
    if not event:
        return None

    sub = event.get('headers', {}).get('sub')
    if sub:
        return sub

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
