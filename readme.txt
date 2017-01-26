Readme
------
1. Run the script 'copy_authors_and_trim_names.py' to generate the 'AuthorsTrimmed.txt' file with trimmed names.
	$ ./copy_authors_and_trim_names.py
2. Make sure the file 'prevpos.txt' exists and is empty. If it exists and is not empty, remove it using 'rm prevpos.txt' and then create an empty one using 'touch prevpos.txt'. If it does not exist, only use the touch command to create a blank one.
3. Update the paths of the files 'AuthorsTrimmed.txt', 'PaperAuthorAffiliations.txt' and 'PaperReferences.txt' in the file 'algo.py'.
4. Make sure the files exist at their respective paths and are valid.
5. Make sure the machine has internet connectivity.
6. Run the script at the shell prompt as follows:
	$ ./algo.py
7. At the prompt that appears, enter the password of the Neo4j user and hit enter.
