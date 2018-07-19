#!/bin/bash -e
if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    openssl aes-256-cbc -K $encrypted_a4e132a4b597_key -iv $encrypted_a4e132a4b597_iv \
        -in travis/codesigning.asc.enc -out travis/codesigning.asc -d
    gpg --fast-import travis/codesigning.asc
  ./gradlew publish --info --stacktrace closeAndReleaseRepository -PossrhUsername=${OSSRH_JIRA_USERNAME} -PossrhPassword=${OSSRH_JIRA_PASSWORD} -Psigning.gnupg.password="$GPG_PASSPHRASE" -Psigning.gnupg.keyName="$GPG_KEY_NAME" -Psigning.gnupg.useLegacyGpg=true
fi
