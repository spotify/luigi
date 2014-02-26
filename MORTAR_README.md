# Setting up locally

* Clone the repo. 
    git clone git@github.com:spotify/luigi.git
* Add spotify Remote
    git remote add spotify git@github.com:spotify/luigi.git
* Checkout upstream branch
    get checkout upstream
* Set upstream to track spotify master
    git branch upstream -u spotify/master

# Pull in latest changes from spotify

* Check out upstream branch
    git checkout upstream
* Get latest changes from Spotify (assuming tracking was set up)
    git pull 
* Checkout master
    git checkout master
    git pull
* Merge in spotify branch
    git merge upstream
