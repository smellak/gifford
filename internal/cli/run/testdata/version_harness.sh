#!/bin/sh
case "${0##*/}" in
  version)
    printf '%s\n' 'fakeharness 9.9.9'
    ;;
  blank)
    printf '\n'
    ;;
  blankfirst)
    printf '\n%s\n' 'fakeharness 1.2.3'
    ;;
  slow)
    sleep 30
    ;;
  coldstart)
    sleep 3
    printf '%s\n' 'fakeharness 3.0.0'
    ;;
  control)
    printf 'a\tb\n'
    ;;
  wide64)
    printf '%s\n' 'éééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééé'
    ;;
  wide65)
    printf '%s\n' 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
    ;;
  *)
    exit 2
    ;;
esac
