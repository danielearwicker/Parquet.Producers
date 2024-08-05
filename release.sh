#!/bin/bash
set -e

node apply-version.js
rm -rf bin
rm -rf obj
dotnet build --configuration Release
dotnet test --configuration Release Parquet.Producers/Parquet.Producers.csproj

export VER=`cat .version`
dotnet nuget push Parquet.Producers/nupkg/Parquet.Producers.$VER.nupkg -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json
