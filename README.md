# Amanda Website
Amanda Website gives information about Amanda, the latest updates to the project, links to documentation, binaries and information on how to contribute. The site is developed using the Hugo framework (https://gohugo.io/).

# Prerequites
Download and Install Hugo (https://gohugo.io/getting-started/installing/)

# Development
The amanda website is built using a static site generator. The content for the website is written in markdown which is then compiled into a static webpage with the help of Hugo framework. The following points gives the information about the general layout of the site sources:
* To edit the content of the site edit the markdown files under content directory
* To change the styling of the site edit custom.css under static/css directory
* To change the layout of the site edit the files under layout folder
### Note
To know more about the theme used for the site refer https://themes.gohugo.io/hugo-theme-learn/ or the README file under themes/hugo-theme-learn/ directory. 

# Running the site locally
To run the site locally:
* Open terminal or command prompt
* change to the directory with the source files
* Run "hugo server" command. 
This will start a local server and render the site

# Building and Deploying
## Build 
To build the site
* Open terminal or command prompt
* change to the directory with the source files
* Run "hugo" command. 
This will generate a public folder with the compiled files

## Deploying
To deploy the site push the contents of the public folder to the gh-pages branch of this repository. This will automatically deploy the new changes
