
# Analyzing Patterns and Outcomes of Crimes and Arrests in Urban Areas of Los Angeles in USA
### Description
This project leverages a comprehensive dataset of crime reports to explore patterns and correlations within urban crime and law enforcement activities in Los Angeles from 2020 to present. The project intends to optimize policing tactics and improve public safety measures by applying data-driven insights. The analysis aims to ascertain if aggressive law enforcement actions are linked to noticeable drops in crime by comparing the number and kind of arrests with the crime rates in particular neighborhoods. This analysis is crucial for city planners, police departments, and community leaders striving to allocate resources effectively and improve safety in urban environments.
### Key Analysis
- Correlation between the frequency of arrests and crime rates.
- victim demographics (age, sex, descent) for specific types of crimes.
- Crime Analysis ( Top crime, Top crime location, Distribution of Crime by location).
### Findings
The data suggests that while crime rates and arrests are weakly correlated, they do not necessarily predict one another directly, underscoring the need for a multifaceted approach to crime prevention and law enforcement that goes beyond mere surveillance or patrolling. Instead, more emphasis might be needed on community engagement, policy reform, and resource allocation tailored to the dynamics of specific areas and demographic groups.Victim demographics highlight a critical area for intervention, showing that different groups are uniquely vulnerable to certain crimes. These insights should drive specialized support services and prevention programs, focusing on the most at-risk groups identified in the data—children in family-related crimes and the elderly in financial crimes.


# Methods of Advanced Data Engineering Template Project

This template project provides some structure for your open data project in the MADE module at FAU.
This repository contains (a) a data science project that is developed by the student over the course of the semester, and (b) the exercises that are submitted over the course of the semester.

To get started, please follow these steps:
1. Create your own fork of this repository. Feel free to rename the repository right after creation, before you let the teaching instructors know your repository URL. **Do not rename the repository during the semester**.

## Project Work
Your data engineering project will run alongside lectures during the semester. We will ask you to regularly submit project work as milestones, so you can reasonably pace your work. All project work submissions **must** be placed in the `project` folder.

### Exporting a Jupyter Notebook
Jupyter Notebooks can be exported using `nbconvert` (`pip install nbconvert`). For example, to export the example notebook to HTML: `jupyter nbconvert --to html examples/final-report-example.ipynb --embed-images --output final-report.html`


## Exercises
During the semester you will need to complete exercises using [Jayvee](https://github.com/jvalue/jayvee). You **must** place your submission in the `exercises` folder in your repository and name them according to their number from one to five: `exercise<number from 1-5>.jv`.

In regular intervals, exercises will be given as homework to complete during the semester. Details and deadlines will be discussed in the lecture, also see the [course schedule](https://made.uni1.de/).

### Exercise Feedback
We provide automated exercise feedback using a GitHub action (that is defined in `.github/workflows/exercise-feedback.yml`). 

To view your exercise feedback, navigate to Actions → Exercise Feedback in your repository.

The exercise feedback is executed whenever you make a change in files in the `exercise` folder and push your local changes to the repository on GitHub. To see the feedback, open the latest GitHub Action run, open the `exercise-feedback` job and `Exercise Feedback` step. You should see command line output that contains output like this:

```sh
Found exercises/exercise1.jv, executing model...
Found output file airports.sqlite, grading...
Grading Exercise 1
	Overall points 17 of 17
	---
	By category:
		Shape: 4 of 4
		Types: 13 of 13
```
