%define name clickhouse-mysql
%define version 0.0.20180321
%define release 1

Summary: MySQL to ClickHouse data migrator
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{version}.tar.gz
License: MIT
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Altinity (Vladislav Klimenko <sunsingerus@gmail.com>)
Packager: Altinity (Vladislav Klimenko <sunsingerus@gmail.com>)
Url: https://github.com/altinity/clickhouse-mysql-data-reader
Requires: python34
Requires: python34-devel
Requires: python34-libs
Requires: python34-pip
Requires: python34-setuptools
Requires: clickhouse-client
Requires: mysql-community-devel
Requires: gcc
Buildrequires: python34
Buildrequires: python34-devel
Buildrequires: python34-libs
Buildrequires: python34-pip
Buildrequires: python34-setuptools

%description
MySQL to ClickHouse data migrator

%prep
set -x
%setup -n %{name}-%{version} -n %{name}-%{version}

%build
set -x
python3 setup.py build

%install
set -x
#python3 setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
python3 setup.py install --single-version-externally-managed -O1 --root=%{buildroot} --record=INSTALLED_FILES --prefix=/usr

mkdir -p %{buildroot}/etc/clickhouse-mysql
mkdir -p %{buildroot}/etc/init.d
#mkdir -p %{buildroot}/etc/systemd/system
mkdir -p %{buildroot}/var/run/clickhouse-mysql
mkdir -p %{buildroot}/var/log/clickhouse-mysql

cp %{_builddir}/%{buildsubdir}/clickhouse_mysql.etc/clickhouse-mysql.conf           %{buildroot}/etc/clickhouse-mysql/clickhouse-mysql-example.conf
cp %{_builddir}/%{buildsubdir}/clickhouse_mysql.init.d/clickhouse-mysql         %{buildroot}/etc/init.d/clickhouse-mysql
#cp %{_builddir}/%{buildsubdir}/init.d/clickhouse-mysql.service %{buildroot}/etc/systemd/system/clickhouse-mysql.service

%clean
rm -rf %{buildroot}

%files -f INSTALLED_FILES
%defattr(-,root,root)
/etc/clickhouse-mysql/clickhouse-mysql-example.conf
/etc/init.d/clickhouse-mysql
#/etc/systemd/system/clickhouse-mysql.service
/var/run/clickhouse-mysql
/var/log/clickhouse-mysql

%post
CLICKHOUSE_USER=clickhouse
CLICKHOUSE_GROUP=${CLICKHOUSE_USER}
CLICKHOUSE_DATADIR=/var/lib/clickhouse

function create_system_user()
{
	USER=$1
	GROUP=$2
	HOMEDIR=$3

	echo "Create user ${USER}.${GROUP} with datadir ${HOMEDIR}"

	# Make sure the administrative user exists
	if ! getent passwd ${USER} > /dev/null; then
		adduser \
			--system \
			--no-create-home \
			--home ${HOMEDIR} \
			--shell /sbin/nologin \
			--comment "Clickhouse server" \
			clickhouse > /dev/null
	fi

	# if the user was created manually, make sure the group is there as well
	if ! getent group ${GROUP} > /dev/null; then
		addgroup --system ${GROUP} > /dev/null
	fi

	# make sure user is in the correct group
	if ! id -Gn ${USER} | grep -qw ${USER}; then
		adduser ${USER} ${GROUP} > /dev/null
	fi

	# check validity of user and group
	if [ "`id -u ${USER}`" -eq 0 ]; then
		echo "The ${USER} system user must not have uid 0 (root). Please fix this and reinstall this package." >&2
	        exit 1
	fi

	if [ "`id -g ${GROUP}`" -eq 0 ]; then
		echo "The ${USER} system user must not have root as primary group. Please fix this and reinstall this package." >&2
	        exit 1
	fi
}

create_system_user $CLICKHOUSE_USER clickhouse

chown -R $CLICKHOUSE_USER:$CLICKHOUSE_GROUP /var/run/clickhouse-mysql
chown -R $CLICKHOUSE_USER:$CLICKHOUSE_GROUP /var/log/clickhouse-mysql

/usr/bin/pip3 install mysqlclient
/usr/bin/pip3 install mysql-replication
/usr/bin/pip3 install clickhouse-driver
/usr/bin/pip3 install configobj

