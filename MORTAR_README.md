# Setting up locally

* Clone the repo. 
* Add spotify Remote
* Checkout upstream branch
* Set upstream to track spotify master

        git clone git@github.com:spotify/luigi.git
        git remote add spotify git@github.com:spotify/luigi.git
        get checkout upstream
        git branch upstream -u spotify/master

# Pull in latest changes from spotify

* Check out upstream branch
* Get latest changes from Spotify (assuming tracking was set up)
* Checkout master
* Merge in spotify branch

        git checkout upstream
        git pull 
        git checkout master
        git pull
        git merge upstream

#Sending Pull Requests upstream

To create pull requests for spotify/luigi, we'll branch off of our "upstream" branch, apply a patch there, and then send the pull request to spotify/luigi#master. This will ensure that changes always apply cleanly to spotify.
