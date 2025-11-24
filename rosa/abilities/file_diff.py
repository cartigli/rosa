"""
Test script for the diff_match_patch library.
"""

import diff_match_patch as dmp_mod

def reverse_patch(old, new):
    # initiate the diff engine
    dmp = dmp_mod.diff_match_patch()

    # reverse patch - diff from new to old
    patches = dmp.patch_make(new, old)

    # convert to text 
    patch_text = dmp.patch_toText(patches)

    return patch_text


def apply_patch(new, patch_text):
    # initiate the diff engine
    dmp = dmp_mod.diff_match_patch()

    # convert back to patch obj
    patches = dmp.patch_fromText(patch_text)

    # apply patch to new file to get og
    original_data = dmp.patch_apply(patches, new)

    original = original_data[0]
    success = original_data[1]

    if all(success):
        return original
    else:
        print(success)
        raise Exception('Error applying patches to file.')


def read_file(path):
    with open(path, 'r') as f:
        file = f.read()
    
    if file:
        return file


if __name__=="__main__":
    one = read_file("/home/tom/one")
    two = read_file("/home/tom/two")
    three = read_file("/home/tom/three")
    four = read_file("/home/tom/four")

    patch_one = reverse_patch(one, two)
    patch_two = reverse_patch(two, three)
    patch_three = reverse_patch(three, four)

    # now imagine we only have the fourth file version & the three patches
    # we need to get version one from this data
    version_three = apply_patch(four, patch_three)
    version_two = apply_patch(version_three, patch_two)
    version_one = apply_patch(version_two, patch_one)

    # patches = reverse_patch(old, new)
    # original = apply_patch(new, patches)
