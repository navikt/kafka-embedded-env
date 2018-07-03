#!/usr/bin/bash -e
if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
  ./gradlew publish closeAndReleaseRepository -PossrhUsername=${OSSRH_JIRA_USERNAME} -PossrhPassword=${OSSRH_JIRA_PASSWORD} -Psigning.keyId=${GPG_KEY_NAME} -Psigning.password=${GPG_PASSPHRASE} -Psigning.secretKeyRingFile=codesigning.gpg
fi
