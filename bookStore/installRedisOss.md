apt update && apt install -y \
  build-essential \
  ca-certificates \
  git \
  pkg-config \
  libssl-dev \
  tcl \
  gawk \
  python3 \
  python3-pip \
  autoconf \
  automake \
  libtool \
  clang \
  libclang-dev \
  unzip \
  curl \
  wget \
  gpg

wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' > /etc/apt/sources.list.d/kitware.list
apt update && apt install -y kitware-archive-keyring cmake

cd /usr/src
rm -rf redis-8.8-m03
git clone --branch 8.8-m03 --depth 1 https://github.com/redis/redis.git redis-8.8-m03
cd redis-8.8-m03

unset PREFIX LIBDIR PKG_CONFIG_PATH CFLAGS CPPFLAGS LDFLAGS
export LIBCLANG_PATH=/usr/lib/llvm-14/lib

make distclean || true

make -j"$(nproc)" \
  BUILD_TLS=yes \
  BUILD_WITH_MODULES=yes \
  INSTALL_RUST_TOOLCHAIN=yes \
  DISABLE_WERRORS=yes \
  all
