[TO DO]

remove extraneous imports from scripts - and the lower if __name__=="__main__": block (give: ok, contrast: ok, get: ok)

make another lib file for diff'ing - adjust imports accordingly

modify confirmations and checks to accept and check its status (give[commit: ok], contrast[ask_to_show: ok])

format the scripts to take more universal formats / types

timing discrepancy check before hashing - efficiency (give: ok, contrast: ok)

New libs:
- dispatch: initiates the logger and connection obj. w.a context manager (260, 6)
    - operator ('operator, I need...'), admin, 

- analyst: scans local & remote data before comparing the sources of information (264, 8)
    - scanner, overview, diff[r]

- technician: edits the server & has functions for batched for uploading (228, 9)
    - collects batched, populates them, & uploads them (then commit & counter)
    - tech, spiderman, upstairs, giver

- contractor: edits the disk and has functions for batched downloading (378, 12)
    - includes fat_boy contextmanager, could be moved to managing file or with phones()
    - contract, builder, filer, getter, foreman, 