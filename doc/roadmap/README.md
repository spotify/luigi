# Luigi Roadmap

Uldis (@ulzha) once jotted down the thoughts of having a luigi roadmap:

> The roadmap would consist of highly prioritized features that Chief Maintainers
> would plan to actively work on themselves and appreciate help with most, and
> then some guidelines towards consistent implementation practices and quality
> assurance policies, some such. Also with the roadmap we could start to
> explicitly plan the features life- and deprecation cycles that result in
> breaking changes. I think 3.0 series would be a good opportunity to get rid of
> a great deal of tech debt that mostly exists for backward compatibility. By
> that I don't have any use-cases we would stop supporting in my mind, just that
> we would refactor massively so that users can still achieve the desired
> behaviors using perhaps slightly different features, yet ones that are
> consistent, generic and maintainable.
> 
> I wonder what would be a good communication format. Something like doc/roadmap/README.md, and cross-posting of updates in luigi-user@?


Later on we could consider one file per idea under `doc/roadmap/` and this
`README.md` would be an overview. But for now it's an monolithic file.

## Sanitizing log outputs

For sanitizing console output and logging. Sounds great! I I really want to get rid of our own "luigi-interface" convention.
Other priorities we have that are being worked on by DataEx (Uldis' squad) is sanitizing console output & logging, and removing the dependency on cron

## Cron replacement

Arash thinks this is a quite big task (in contrast to the other things in this
roadmap). But in the very long run, this could become relevant.

## Unify task namespaces with python namespaces

Arash previously wrote in an email to Uldis et al:

> Another thing I want to do, and I think Spotify need it even more than me is to unify `task_namespace` with __name__ (as a default). I'm willing to implement 100% of this as I think I'll need it myself. In particular I want this because my cron lines currently look like:
> 
>     luigi --module my.module my.module.MyTask --params blah
> 
> Indeed, I always run `luigi.namespace(__name__)` in the beginning of my py-files. If this was a recognized convention/default in luigi. We could easily make cron lines look like
> 
>     luigi my.module.MyTask --params blah
> 
> I just saw I suggested this half a year ago here: https://github.com/spotify/luigi/pull/1075#issuecomment-124142403 :D. Do you guys like this idea? As I said I'm willing to implement it.

Anyone could do this of course.

## Merging the two "kinds" of DISABLED state

This already has an issue: https://github.com/spotify/luigi/issues/1611

## Make scheduler not prune FAILED and DISABLED tasks

Currently the scheduler "forgives" a failed tasks cooldown if no worker cares
about it and it gets pruned.  This totally kills the point of punishing failed
tasks.

## Use a state-machine library for scheduler

Refactor states in the scheduler using a state-machine library
perhaps use https://github.com/tyarkoni/transitions ??

## Clarify what luigi's public API is

Find out how to make all documentation only show public methods. readthedocs
displays quite much more than what should be displayed.

## Temporary files in luigi

Temporary files is something most luigi plumbers have to deal with. And many of
the libraries for the many different file systems come with some support.
However the implementations are different, worse, so are even the interfaces.

Arash once wrote a detailed plan for this which already has a [dedicated
issue](https://github.com/spotify/luigi/issues/1519).

## Separate out "contrib" of luigi

spotify/luigi is not a big repository. The community is big and that's great,
but now it have started to put an unreasonable burden on the few maintainers.
As the maintainers need to review PRs for all kinds of contrib modules.

At some point we had this [issue](https://github.com/spotify/luigi/issues/451)
discussing this. But As of today (Oct 2016) luigi is even bigger and it feels
clear to Arash that we need to seperate things even more.

A *small* start for anyone interested is to move out for example `luigi/s3.py`
into `luigi/contrib/s3.py`.

## Insignificant versus secret parameters

Every now and then there are pull requests wanting to change the behavior of
significant parameters. Basically some users would like the insignificant
parameters to still be shown in the schedulers while others think they
shouldn't.  The behavior have been changing and reversed a few times.

 * https://github.com/spotify/luigi/pull/1891
 * https://github.com/spotify/luigi/pull/1816
 * https://github.com/spotify/luigi/pull/1109
 * Arash remembers there even more, if you know of any please add them here.

Arash belives we should consider if a luigi user should specify if parameter is
secret or not.  i.e. a seperate dimension from if it's significant or not. Or
perhaps a one-out-of-three option.
