Clarisse: wrapper for repeated and concurrent codeml execution
==================================================

Clarisse is a [Ruby](https://www.ruby-lang.org/en/) script that simplifies the repeated and parallel execution of [codeml](http://envgen.nox.ac.uk/bioinformatics/docs/codeml.html) over a large number of alignments. 

A single configuration file can define multiple sets of options, including file
globs and extraction of previous results, which will be used to generate the
necessary control files to run codeml with.

BASIC USAGE
---------------------------------------------------

Below is a typical analysis with codeml, where files 'seq.phy' and 'tree.nwk' will be used as input and 'config.ctl' is the control file for codeml that includes options that point to the previous two files.

    alignment_A/
    ├── config.ctl
    ├── seq.phy
    └── tree.nwk

    $ cd alignment_A
    $ codeml config.ctl

A common need during phylogenetic analysis is to repeat the above process after adjusting options in the control file. Clarisse makes this easier by using a configuration file that describes a different set of options to be used on consecutive codeml runs. A regular codeml control file specifies a single set of options:

    $ cat config.ctl
         seqfile = seq.phy
        treefile = tree.nwk
           model = 1
           omega = 0
           alpha = 1
         outfile = results.out
            ...

On the other hand, a Clarisse configuration file describes the same options, but grouped by an iteration number:

    $ cat iterative.ctl
            [1]
         seqfile = seq.phy
        treefile = tree.nwk
           model = 1
           omega = 0
           alpha = 1
         outfile = results_1.out
            ...

            [2]
         seqfile = seq.phy
        treefile = tree.nwk
           model = 0
           omega = 1
           alpha = 0
         outfile = results_2.out
            ...

Clarisse can be given the new iterative configuration file and the directory where it should be run

    $ clarisse iterative.ctl alignment_A

and it will automatically run codeml with the two different sets of options. The result of this will be that codeml will generate the files 'results_1.out' and 'results_2.out' inside alignment_A.  Clarisse will create a work directory called '__clarisse' inside alignment_A where it will store the individual codeml control files generated and the output that codeml printed to stdout and stderr (if any) for every iteration. There is no limit on the number of iterations.

CONCURRENT ANALYSIS
--------------------------------------------------
When given more than one directory, Clarisse will generate control files for all iterations and all directories and run codeml on all of them.  Analysis of different directories is sequential by default, but if the '--threads' option is used, the alignments in different directories will be analyzed in parallel.  For example, if the directories alignment_* contain the '.seq' and '.nwk' files for four different alignments and the configuration file 'config.ctl' defines an N number of iterations to execute, the command

$ clarisse --threads 4 config.ctl alignment_1 alignment_2 alignment_3 alignment_4

will create four threads and run codeml in parallel on each of the alignments in the four directories, each thread executing N iterations on a directory.


FILENAME EXPANSION
--------------------------------------------------

The fields 'seqfile' and 'treefile' on Clarisse's configuration file accepts globs. They will be resolved based on the execution directories. Given the directory structure below

    /path/to/alignments/
    ├── config.ctl
    ├── tree.nwk
    ├── alignment_1
    │   └── seq_1.phy
    ├── alignment_2
    │   └── seq_2.phy
    ├── alignment_3
    │   └── seq_3.phy
    └── alignment_4
        └── seq_4.phy

and the below options on file 'config.ctl'

        [1]
     seqfile = *.phy
    treefile = ../*.nwk
        ...

        [2]
     seqfile = *.phy
    treefile = ../*.nwk
        ...

then Clarisse will correctly resolve the paths to the sequence file on each directory and the common tree file for every control file generated.


COMMON OPTIONS
--------------------------------------------------

In addition to the numbered iterations, a Clarisse configuration file can contain a common section by using the header '[*]' that will complement the options in every iteration, ignoring any option that would become duplicate. For example, given the two iterations and common options on the left, the common options will be copied to the iteration options as shown on the right.

            [1]                        ->                     [1]
           model = 1                   ->                 seqfile = seq.phy
           omega = 0                   ->                treefile = tree.nwk
           alpha = 1                   ->                   model = 1
         outfile = 1.out               ->                   omega = 0
            ...                        ->                   alpha = 1
                                       ->                 outfile = 1.out
            [2]                        ->                     ...
           omega = 1                   ->
         outfile = 2.out               ->
            ...                        ->                     [2]
                                       ->                  seqfile = seq.phy
            [*]                        ->                 treefile = tree.nwk
           model = 0                   ->                    model = 0
           alpha = 0                   ->                    omega = 1
         seqfile = seq.phy             ->                    alpha = 0
        treefile = tree.nwk            ->                  outfile = 2.out

ITERATIVE OPTION ADJUSTMENT
--------------------------------------------------

Clarisse allows using the result from a previous iteration as the value in some of the options from the second iteration forwards.

         [2]
      seqfile = *.phy
     treefile = *.nwk
        kappa = [1]       #  Value for 'kappa' will be extracted from the results of the first iteration
        omega = 0
         ...

CLARISSE CONFIGURATION FILE
--------------------------------------------------

The rules for the contents of the configuration file are:

- Empty lines are allowed.
- Non-empty lines must contain either a section header, an option or a comment.
- Each option must contain one key/value pair separated by '='.
- Comments begin with '#'.
- Section headers must be enclosed in square brackets and must be either a positive integer or the character '*'.
- The section header [1] is mandatory. If other numerical headers are to be used, they must follow [1] and their number be consecutive.
- A section header marks the start of a section and subsequent options belong to that section until the next header is reached.
- Every option must appear inside a section and each section must contain at least one option.


Configuration file format
--------------------------------------------------
See the [wiki](https://github.com/tripplab/clarisse_ruby/wiki)
