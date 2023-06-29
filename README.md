# Data Engineering Test Assignment

This repo contains the solution for test assignment.

## Installation

Please put the file [dataset.txt](https://drive.google.com/file/d/1Ne_1vHsY-qh7ORG-pcAV98q4b8eay57W/view?usp=sharing) in dataset folder for app to work

Open CMD/Terminal on main folder in repo and Build docker image

```bash
docker image build . -t assignment-image
```

## Usage

```bash
docker run assignment-image
```

## Assumptions
1. There is a data contract with source.
