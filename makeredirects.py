#!/usr/bin/env python

"""
This script takes a file on S3 and creates a redirect from every possible
permutation of case to the original file.

Author: Michael Wirth (https://github.com/mikewirth/s3-caseredirect/)
"""

import sys
import os.path
import argparse

try:
    import boto.s3.connection

except:
    print "boto library (http://code.google.com/p/boto/) for aws needs to be installed"
    sys.exit(1)

filenames = None


def make_case_insensitive(bucket, access, secret, key):
    """ Get filename permutations """
    global filenames
    filenames = []
    filename = os.path.basename(key)
    path = os.path.dirname(key)

    filename_permutations(filename)

    connection = boto.s3.connection.S3Connection(access, secret, True)
    b = connection.get_bucket(bucket)
    
    for fname in filenames:
        if fname == filename:
            continue
        
        k = b.new_key(os.path.join(path, fname))
        k.set_redirect(key)


def filename_permutations(filename, pos=0):
    if len(filename) == pos:
        filenames.append(filename)
    else:
        upper = filename[:pos] + filename[pos:pos+1].upper() + filename[pos+1:]
        lower = filename[:pos] + filename[pos:pos+1].lower() + filename[pos+1:]

        if upper != lower:
            filename_permutations(upper, pos+1)
            filename_permutations(lower, pos+1)
        else:
            filename_permutations(filename, pos+1)


def main():
    """ CLI """
    parser = argparse.ArgumentParser()
    
    parser.add_argument("access", help="AWS credentials: access code")
    parser.add_argument("secret", help="AWS credentials: secret")
    parser.add_argument("bucket", help="Name of Amazon S3 bucket")
    parser.add_argument("key", help="Name of the key to make available case-insensitively. (Starts with a slash.)")

    args = parser.parse_args()

    make_case_insensitive(args.bucket, args.access, args.secret, args.key)

if __name__ == "__main__":
    main()
