#!/bin/sh
case "${0##*/}" in
  version)
    printf '%s\n' 'fakeharness 9.9.9'
    ;;
  blank)
    printf '\n'
    ;;
  slow)
    sleep 30
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
