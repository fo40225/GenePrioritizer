# 請參閱 https://aka.ms/customizecontainer 了解如何自訂您的偵錯容器，以及 Visual Studio 如何使用此 Dockerfile 來組建您的映像，以加快偵錯速度。

# 這些 ARG 允許交換從 VS 偵錯時用來製作最終映像的基礎映像
ARG LAUNCHING_FROM_VS
# 這將設定最終的基礎映像，但前提是已定義 LAUNCHING_FROM_VS
ARG FINAL_BASE_IMAGE=${LAUNCHING_FROM_VS:+aotdebug}

# 此階段用於以快速模式從 VS 執行時 (偵錯設定的預設值)
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
USER app
WORKDIR /app


# 此階段是用來組建服務專案
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
# 安裝 clang/zlib1g-dev 相依性以發佈至原生
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    clang zlib1g-dev
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Initdb/Initdb.csproj", "Initdb/"]
RUN dotnet restore "./Initdb/Initdb.csproj"
COPY . .
WORKDIR "/src/Initdb"
RUN dotnet build "./Initdb.csproj" -c $BUILD_CONFIGURATION -o /app/build

# 此階段可用來發佈要複製到最終階段的服務專案
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Initdb.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=true

# 從 VS 啟動以支援一般模式的偵錯時，會使用此階段做為最終階段的基礎映像 (未使用偵錯設定時的預設值)
FROM base as aotdebug
USER root
# 安裝 GDB 以支援原生偵錯
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gdb
USER app

# 此階段用於生產環境，或以一般模式從 VS 執行時 (未使用偵錯設定時的預設值)
FROM ${FINAL_BASE_IMAGE:-mcr.microsoft.com/dotnet/runtime-deps:8.0} AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["./Initdb"]