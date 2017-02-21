# Clarisse: a wrapper for automatic codeml execution
==================================================
Clarisse is a script that automates the execution of [codeml] (http://envgen.nox.ac.uk/bioinformatics/docs/codeml.html), from PAML [http://abacus.gene.ucl.ac.uk/software/paml.html], over a number of directories. It can copy or generate the necessary control files and then use them to execute codeml. It has three execution modes that define the control file to be used:
- Copying a source control file to every directory. This is specified with the option --control
- Using a control file that already exists on every directory and that shares the same name. The existing control file name is specified with --existing
- Generating multiple control files given a configuration file with the --configuration and --template options. This has the advantage that it can generate more than one control file and execute codeml once for every file in each directory. It also allows to use results from previous runs to generate new control files.

Additionally, it can generate a results.csv file that concentrates the results of every directory.

Use the --help option to display possible parameters

> $ clarisse --help

Configuration file format
--------------------------------------------------
TODO

Results configuration file format
--------------------------------------------------
TODO

