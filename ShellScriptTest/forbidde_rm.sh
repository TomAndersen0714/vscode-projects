#!/bin/bash
alias rm=trash
alias rl='ls ~/.Trash'
alias ur=undelfile
undelfile() {
    mv -i ~/.Trash/$@ ./
}
trash() {
    mv $@ ~/.Trash/
}
cleartrash() {
    read -p "Clear trash?[n]" confirm
    [ $confirm == 'y' ] || [ $confirm == 'Y' ] && /usr/bin/rm -rf ~/.Trash/*
}
