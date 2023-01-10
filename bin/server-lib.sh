#!/usr/bin/env sh
# 버전설정. 버전이 올라가면 변경한다.
VERSION=1.11.0
# 스프링과 충돌이 날만한 클래스는 지워준다.
zip -q -d ../target/indexer-$VERSION.jar.original "*/config/*" "*/AsyncController*" "*/CommandController*" "*/IndexerApplication*"
# indexer-1.11.0-lib.jar 같은 이름으로 바꿔준다.
mv ../target/indexer-$VERSION.jar.original ../target/indexer-$VERSION-lib.jar