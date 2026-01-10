import os
import binascii
import sys

try:
    # Ensure directory exists for local testing
    if not os.path.exists('publisher_data'):
        os.makedirs('publisher_data')

    path = 'publisher_data/secret_key'
    # Mocking secret for local test if not set
    secret = os.environ.get('PUBLISHER_SECRET', 'deadbeef' * 8) 
    
    if not secret:
        print('Error: PUBLISHER_SECRET env var is empty')
        sys.exit(1)
    
    # Clean the secret
    hexval = secret.strip().replace(' ', '').replace('\n', '').replace('\r', '')
    print(f'Secret length: {len(hexval)}')
    
    if len(hexval) % 2 != 0:
        print('Error: Secret length is odd, cannot be hex string')
        sys.exit(1)
        
    try:
        data = binascii.unhexlify(hexval)
        with open(path, 'wb') as f:
            f.write(data)
        print('Secret key imported successfully')
    except binascii.Error as e:
        print(f'Error decoding hex: {e}')
        sys.exit(1)
except Exception as e:
    print(f'Unexpected error: {e}')
    sys.exit(1)
