#!/bin/bash

# general build stuff
yum update -y \
	&& yum groupinstall -y "Development Tools" \
	&& yum install -y wget tar

# libvips needs libwebp 0.5 or later and the one on amazonlinux2 is 0.3.0, so
# we have to build it ourselves

# packages needed by libwebp
yum install -y \
	libjpeg-devel \
	libpng-devel \
	libtiff-devel \
	libgif-devel 

# stuff we need to build our own libvips ... this is a pretty basic selection
# of dependencies, you'll want to adjust these
# dzsave needs libgsf
yum install -y \
	libpng-devel \
	poppler-glib-devel \
	glib2-devel \
	libjpeg-devel \
	expat-devel \
	zlib-devel \
	orc-devel \
	lcms2-devel \
	libexif-devel \
	libgsf-devel

# openslide is in epel -- extra packages for enterprise linux
yum install -y \
	https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
yum install -y \
	openslide-devel 

# non-standard stuff we build from source goes here
VIPSHOME=/usr/local/vips
PKG_CONFIG_PATH=$VIPSHOME/lib/pkgconfig

WEBP_VERSION=1.1.0
WEBP_URL=https://storage.googleapis.com/downloads.webmproject.org/releases/webp

cd /usr/local/src \
	&& wget $WEBP_URL/libwebp-$WEBP_VERSION.tar.gz \
	&& tar xzf libwebp-$WEBP_VERSION.tar.gz \
	&& cd libwebp-$WEBP_VERSION \
	&& ./configure --enable-libwebpmux --enable-libwebpdemux \
		--prefix=$VIPSHOME \
	&& make V=0 \
	&& make install

VIPS_VERSION=8.10.5
VIPS_URL=https://github.com/libvips/libvips/releases/download

cd /usr/local/src \
	&& wget $VIPS_URL/v$VIPS_VERSION/vips-$VIPS_VERSION.tar.gz \
	&& tar xzf vips-$VIPS_VERSION.tar.gz \
	&& cd vips-$VIPS_VERSION \
	&& ./configure --prefix=$VIPSHOME \
	&& make V=0 \
	&& make install

ls $VIPSHOME/lib